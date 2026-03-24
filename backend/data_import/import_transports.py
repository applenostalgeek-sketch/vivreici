"""
Calcul du score accessibilité transports pour chaque commune.

Source : Liste des gares SNCF (data.gouv.fr) — 3 279 gares voyageurs avec coords WGS84.

Méthode :
- Pour chaque commune, calcul de la distance à la gare voyageurs la plus proche
- Les communes avec une gare sur leur territoire : distance = 0
- Score = percentile inverse de la distance (moins c'est loin, mieux c'est)
"""

import asyncio
import sys
import os
import io
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global


GARES_URL = "https://www.data.gouv.fr/fr/datasets/r/d22ba593-90a4-4725-977c-095d1f654d28"
CHUNK_SIZE = 2000  # nb communes par chunk dans la matrice haversine


def haversine_min_distance(lats_q, lons_q, lats_ref, lons_ref):
    """
    Pour chaque point query, retourne la distance min en km vers les points ref.
    Traitement par chunks pour limiter la RAM.
    """
    R = 6371.0
    min_dists = np.full(len(lats_q), np.inf)

    for start in range(0, len(lats_q), CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, len(lats_q))
        lat_q = np.radians(lats_q[start:end])
        lon_q = np.radians(lons_q[start:end])
        lat_r = np.radians(lats_ref)
        lon_r = np.radians(lons_ref)

        dlat = lat_r[None, :] - lat_q[:, None]
        dlon = lon_r[None, :] - lon_q[:, None]
        a = (np.sin(dlat / 2) ** 2
             + np.cos(lat_q[:, None]) * np.cos(lat_r[None, :]) * np.sin(dlon / 2) ** 2)
        dist = R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
        min_dists[start:end] = dist.min(axis=1)

    return min_dists


async def telecharger_gares() -> pd.DataFrame:
    """Télécharge la liste des gares SNCF voyageurs depuis data.gouv.fr."""
    print("Téléchargement des gares SNCF voyageurs...")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(GARES_URL, timeout=60)
        resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text), sep=";", encoding="utf-8-sig", dtype=str)

    # Filtrer gares voyageurs uniquement
    df = df[df["VOYAGEURS"] == "O"].copy()

    # Coordonnées WGS84
    df["lat"] = pd.to_numeric(df["Y_WGS84"], errors="coerce")
    df["lng"] = pd.to_numeric(df["X_WGS84"], errors="coerce")
    df = df.dropna(subset=["lat", "lng"])

    # Garder uniquement les gares en France métropolitaine (hors DOM-TOM)
    df = df[(df["lat"] >= 41) & (df["lat"] <= 52) & (df["lng"] >= -5) & (df["lng"] <= 10)]

    print(f"  → {len(df)} gares voyageurs avec coordonnées (France métro)")
    return df


async def run():
    await init_db()

    # 1. Télécharger les gares SNCF
    df_gares = await telecharger_gares()
    gare_lats = df_gares["lat"].values
    gare_lons = df_gares["lng"].values

    # 2. Charger toutes les communes avec coordonnées
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT c.code_insee, c.latitude, c.longitude
            FROM communes c
            WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL
        """))
        rows = result.fetchall()

    communes = [(r[0], float(r[1]), float(r[2])) for r in rows]
    print(f"  {len(communes)} communes avec coordonnées")

    codes = [r[0] for r in communes]
    lats = np.array([r[1] for r in communes])
    lons = np.array([r[2] for r in communes])

    # 3. Calcul haversine : distance minimale à une gare voyageurs
    print(f"  Calcul haversine ({len(communes)} communes × {len(df_gares)} gares)...")
    dist_km = haversine_min_distance(lats, lons, gare_lats, gare_lons)

    # Déterminer si la commune a une gare sur son territoire (distance < 3 km)
    # Note : on n'a pas le code INSEE dans ce dataset, on utilise un seuil de proximité
    has_gare = dist_km < 3.0
    nb_gares_communes = has_gare.sum()
    print(f"  {nb_gares_communes} communes avec une gare à moins de 3 km")
    print(f"  Distance médiane (sans gare proche) : {np.median(dist_km[~has_gare]):.1f} km")
    print(f"  Distance p90 (sans gare proche)     : {np.percentile(dist_km[~has_gare], 90):.1f} km")
    print(f"  Distance max                         : {np.max(dist_km):.1f} km")

    # 4. Mettre à jour nb_gares (1 si gare < 3km) + distance_gare_km
    print("Mise à jour nb_gares et distance_gare_km en base...")
    async with async_session() as session:
        await session.execute(text("UPDATE scores SET nb_gares = 0, distance_gare_km = -1"))
        await session.commit()
        for i in range(0, len(codes), 5000):
            batch_codes = codes[i:i+5000]
            batch_has = has_gare[i:i+5000]
            batch_dist = dist_km[i:i+5000]
            for j, code in enumerate(batch_codes):
                await session.execute(text(
                    "UPDATE scores SET nb_gares = :g, distance_gare_km = :d WHERE code_insee = :c"
                ), {"g": int(batch_has[j]), "d": round(float(batch_dist[j]), 1), "c": code})
            await session.commit()
    print(f"  {nb_gares_communes} communes marquées avec gare (<3 km)")

    # 5. Percentile inverse → score 0-100
    n = len(dist_km)
    order = np.argsort(dist_km)
    ranks = np.empty(n, dtype=float)
    ranks[order] = np.arange(n)
    scores_t = np.round(100 * (1 - ranks / (n - 1))).astype(int)
    scores_t = np.clip(scores_t, 0, 100)

    print(f"  Score médian (gare proche <3km) : {np.median(scores_t[has_gare]):.0f}")
    print(f"  Score médian (sans gare proche) : {np.median(scores_t[~has_gare]):.0f}")

    # 6. Sauvegarder score_transports
    print("Sauvegarde score_transports...")
    async with async_session() as session:
        for i in range(0, len(codes), 5000):
            batch = codes[i:i+5000]
            sc = scores_t[i:i+5000]
            for j, code in enumerate(batch):
                await session.execute(text("""
                    UPDATE scores SET score_transports = :s WHERE code_insee = :c
                """), {"s": float(sc[j]), "c": code})
            await session.commit()
            print(f"  → {min(i + 5000, len(codes))}/{len(codes)} mis à jour")

    # 7. Recalculer tous les scores globaux
    print("Recalcul des scores globaux avec transports...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_education,   score_sante,     score_revenus,
                   score_transports
            FROM scores
            WHERE score_equipements >= 0
               OR score_securite    >= 0
               OR score_immobilier  >= 0
               OR score_education   >= 0
               OR score_sante       >= 0
               OR score_revenus     >= 0
               OR score_transports  >= 0
        """))
        rows = result.fetchall()
        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_education", "score_sante", "score_revenus", "score_transports"]
        cat_map = {
            "score_equipements": "equipements",
            "score_securite":    "securite",
            "score_immobilier":  "immobilier",
            "score_education":   "education",
            "score_sante":       "sante",
            "score_revenus":     "revenus",
            "score_transports":  "transports",
        }
        nb_recalc = 0
        for row in rows:
            r = dict(zip(cols, row))
            sous_scores = {
                cat: r[col]
                for col, cat in cat_map.items()
                if r[col] is not None and r[col] >= 0
            }
            if not sous_scores:
                continue
            score, lettre, nb = calculer_score_global(sous_scores)
            await session.execute(text("""
                UPDATE scores
                SET score_global = :sg, lettre = :l, nb_categories_scorees = :nb,
                    updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :c
            """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
            nb_recalc += 1
            if nb_recalc % 5000 == 0:
                await session.commit()
                print(f"  → {nb_recalc} scores globaux recalculés")
        await session.commit()

    print(f"  {nb_recalc} scores globaux recalculés.")
    print("Import transports terminé.")


if __name__ == "__main__":
    asyncio.run(run())
