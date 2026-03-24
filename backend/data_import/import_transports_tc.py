"""
Import arrêts de transport en commun (bus, métro, tram, RER, cars interurbains...).

Source : transport.data.gouv.fr — fichier national agrégé des arrêts (~437 Mo, ~25M lignes)
URL : https://transport.data.gouv.fr/datasets/arrets-de-transport-en-france

Méthode :
- Téléchargement du CSV national (streaming vers fichier temp)
- Déduplication par arrondi lat/lon à 3 décimales (~100m)
- Assignation à la commune la plus proche via KDTree haversine
- Comptage d'arrêts uniques par commune (France métro uniquement)
- Score TC = percentile du nb d'arrêts (plus d'arrêts = mieux)
- Score transport final = 0.5 × score_gare + 0.5 × score_TC

Ce script remplace import_transports.py pour le calcul du score final.
import_transports.py doit être lancé d'abord pour avoir distance_gare_km.
"""

import asyncio
import sys
import os
import tempfile
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
from scipy.spatial import cKDTree
from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score


TC_URL = (
    "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com"
    "/81333/81333.20260113.121610.549772.csv"
)

# France métropolitaine (hors DOM-TOM)
LAT_MIN, LAT_MAX = 41.0, 52.0
LON_MIN, LON_MAX = -6.0, 10.0

CHUNK_SIZE = 300_000


async def telecharger_arrets(tmp_path: str):
    """Télécharge le fichier national des arrêts vers un fichier temporaire."""
    print(f"Téléchargement arrêts TC nationaux (~437 Mo)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        async with client.stream("GET", TC_URL) as resp:
            resp.raise_for_status()
            total = 0
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=4 * 1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
                    if total % (50 * 1024 * 1024) == 0:
                        print(f"  → {total // (1024*1024)} Mo téléchargés...")
    print(f"  → Téléchargement terminé ({total // (1024*1024)} Mo)")


def compter_arrets_par_commune(tmp_path: str, codes: list, comm_lats: np.ndarray, comm_lons: np.ndarray) -> dict:
    """
    Lit le CSV par chunks, déduplique les arrêts, les assigne à la commune la plus proche.
    Retourne un dict {code_insee: nb_arrets_uniques}.
    """
    # KDTree sur coordonnées converties en (x, y, z) cartésien pour distance euclidienne ≈ haversine
    R = 1.0
    xs = R * np.cos(np.radians(comm_lats)) * np.cos(np.radians(comm_lons))
    ys = R * np.cos(np.radians(comm_lats)) * np.sin(np.radians(comm_lons))
    zs = R * np.sin(np.radians(comm_lats))
    tree = cKDTree(np.column_stack([xs, ys, zs]))
    print(f"  → KDTree construit sur {len(codes)} communes")

    # Ensemble global d'arrêts uniques par commune : {idx_commune: set of (rounded_lat, rounded_lon)}
    commune_stops: dict[int, set] = {}

    total_lignes = 0
    total_uniques = 0

    for chunk in pd.read_csv(
        tmp_path,
        chunksize=CHUNK_SIZE,
        usecols=["stop_lat", "stop_lon"],
        dtype={"stop_lat": float, "stop_lon": float},
        on_bad_lines="skip",
        low_memory=False,
    ):
        total_lignes += len(chunk)

        # Filtrer France métropolitaine
        chunk = chunk[
            (chunk["stop_lat"] >= LAT_MIN) & (chunk["stop_lat"] <= LAT_MAX) &
            (chunk["stop_lon"] >= LON_MIN) & (chunk["stop_lon"] <= LON_MAX)
        ].copy()

        if chunk.empty:
            continue

        # Dédupliquer par arrondi à 3 décimales (~100m)
        chunk["rlat"] = chunk["stop_lat"].round(3)
        chunk["rlon"] = chunk["stop_lon"].round(3)
        chunk = chunk.drop_duplicates(subset=["rlat", "rlon"])

        # Convertir en cartésien pour requête KDTree
        rlat_r = np.radians(chunk["rlat"].values)
        rlon_r = np.radians(chunk["rlon"].values)
        xs_q = R * np.cos(rlat_r) * np.cos(rlon_r)
        ys_q = R * np.cos(rlat_r) * np.sin(rlon_r)
        zs_q = R * np.sin(rlat_r)

        _, idxs = tree.query(np.column_stack([xs_q, ys_q, zs_q]))

        # Accumulation par commune
        for i, (rlat, rlon) in enumerate(zip(chunk["rlat"].values, chunk["rlon"].values)):
            comm_idx = idxs[i]
            if comm_idx not in commune_stops:
                commune_stops[comm_idx] = set()
            commune_stops[comm_idx].add((rlat, rlon))

        total_uniques += len(chunk)
        if total_lignes % (CHUNK_SIZE * 5) == 0:
            print(f"  → {total_lignes:,} lignes traitées, {total_uniques:,} arrêts uniques assignés...")

    print(f"  → Total : {total_lignes:,} lignes, {total_uniques:,} arrêts uniques (avant dédup globale)")

    # Compter par commune
    result = {}
    total_final = 0
    for idx, stops in commune_stops.items():
        result[codes[idx]] = len(stops)
        total_final += len(stops)
    print(f"  → {total_final:,} arrêts uniques assignés à {len(result)} communes")
    return result


async def run():
    print("=== Import arrêts TC nationaux (transport.data.gouv.fr) ===\n")
    await init_db()

    # 1. Charger les communes
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT c.code_insee, c.latitude, c.longitude
            FROM communes c
            JOIN scores s ON s.code_insee = c.code_insee
            WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL
        """))
        rows = result.fetchall()

    communes_data = [(r[0], float(r[1]), float(r[2])) for r in rows]
    codes = [r[0] for r in communes_data]
    comm_lats = np.array([r[1] for r in communes_data])
    comm_lons = np.array([r[2] for r in communes_data])
    print(f"  {len(codes)} communes chargées")

    # 2. Télécharger vers fichier temp
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, prefix="tc_arrets_") as tmp:
        tmp_path = tmp.name

    try:
        await telecharger_arrets(tmp_path)

        # 3. Compter les arrêts par commune
        print("\nTraitement des arrêts par commune...")
        nb_arrets = compter_arrets_par_commune(tmp_path, codes, comm_lats, comm_lons)

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
            print("  → Fichier temporaire supprimé")

    # Stats
    counts = np.array([nb_arrets.get(c, 0) for c in codes])
    communes_avec_tc = (counts > 0).sum()
    print(f"\n  Communes avec >=1 arrêt TC : {communes_avec_tc:,} / {len(codes):,}")
    print(f"  Médiane (avec TC) : {np.median(counts[counts > 0]):.0f} arrêts")
    print(f"  P90 : {np.percentile(counts, 90):.0f}, max : {np.max(counts):.0f}")

    # 4. Sauvegarder nb_arrets_tc
    print("\nSauvegarde nb_arrets_tc...")
    async with async_session() as session:
        for i in range(0, len(codes), 5000):
            batch = codes[i:i+5000]
            for code in batch:
                await session.execute(text(
                    "UPDATE scores SET nb_arrets_tc = :n WHERE code_insee = :c"
                ), {"n": nb_arrets.get(code, 0), "c": code})
            await session.commit()
            print(f"  → {min(i+5000, len(codes))}/{len(codes)}")

    # 5. Recalculer score_transports = 0.5 * score_gare + 0.5 * score_TC
    print("\nRecalcul score_transports composite (gare + TC)...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_insee, distance_gare_km, nb_arrets_tc
            FROM scores
        """))
        score_rows = result.fetchall()

    codes_s = [r[0] for r in score_rows]
    dist_arr = np.array([float(r[1]) if r[1] is not None and float(r[1]) >= 0 else np.nan for r in score_rows])
    tc_arr = np.array([int(r[2]) if r[2] is not None else 0 for r in score_rows])

    # Séries valides pour le calcul de percentile
    serie_dist = pd.Series(dist_arr[~np.isnan(dist_arr)])
    serie_tc = pd.Series(tc_arr.astype(float))

    scores_gare = np.array([
        percentile_to_score(d, serie_dist, "inverse") if not np.isnan(d) else -1.0
        for d in dist_arr
    ])
    scores_tc = np.array([
        percentile_to_score(float(n), serie_tc, "direct")
        for n in tc_arr
    ])

    # Composite
    final_scores = np.full(len(codes_s), -1.0)
    for i in range(len(codes_s)):
        sg, st = scores_gare[i], scores_tc[i]
        if sg >= 0 and st >= 0:
            final_scores[i] = round(0.5 * sg + 0.5 * st, 1)
        elif sg >= 0:
            final_scores[i] = sg
        elif st >= 0:
            final_scores[i] = st

    valid = final_scores >= 0
    print(f"  Score transport médian : {np.median(final_scores[valid]):.1f}")
    print(f"  Communes avec score transport : {valid.sum():,}")

    # Sauvegarder
    async with async_session() as session:
        for i in range(0, len(codes_s), 5000):
            for j, code in enumerate(codes_s[i:i+5000]):
                s = float(final_scores[i+j])
                if s >= 0:
                    await session.execute(text(
                        "UPDATE scores SET score_transports = :s WHERE code_insee = :c"
                    ), {"s": s, "c": code})
            await session.commit()
            print(f"  → {min(i+5000, len(codes_s))}/{len(codes_s)} scores transports sauvegardés")

    # 6. Recalcul scores globaux
    print("\nRecalcul scores globaux...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_education, score_sante, score_revenus, score_transports
            FROM scores
            WHERE score_equipements >= 0 OR score_securite >= 0
               OR score_immobilier >= 0 OR score_education >= 0
               OR score_sante >= 0 OR score_revenus >= 0 OR score_transports >= 0
        """))
        global_rows = result.fetchall()
        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_education", "score_sante", "score_revenus", "score_transports"]
        cat_map = {
            "score_equipements": "equipements", "score_securite": "securite",
            "score_immobilier": "immobilier", "score_education": "education",
            "score_sante": "sante", "score_revenus": "revenus", "score_transports": "transports",
        }
        nb_recalc = 0
        for row in global_rows:
            r = dict(zip(cols, row))
            sous_scores = {
                cat: r[col] for col, cat in cat_map.items()
                if r[col] is not None and r[col] >= 0
            }
            if not sous_scores:
                continue
            score, lettre, nb = calculer_score_global(sous_scores)
            await session.execute(text("""
                UPDATE scores SET score_global = :sg, lettre = :l,
                    nb_categories_scorees = :nb, updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :c
            """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
            nb_recalc += 1
            if nb_recalc % 5000 == 0:
                await session.commit()
                print(f"  → {nb_recalc} scores globaux recalculés")
        await session.commit()

    print(f"  → {nb_recalc} scores globaux recalculés")
    print("\n=== Import TC terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
