"""
Import arrêts de transport en commun avec filtre de fréquence.

Source : transport.data.gouv.fr — fichier national agrégé des arrêts (~437 Mo, ~25M lignes)
Dataset : https://www.data.gouv.fr/fr/datasets/arrets-de-transport-en-france/

Méthode (v2 — filtre occurrence + seuil distance adaptatif) :
- Le CSV contient les arrêts de TOUS les opérateurs français concaténés.
- Un même arrêt physique apparaît 1× pour un car scolaire, 10-50× pour un arrêt de métro/RER.

Filtre 1 — Fréquence (MIN_OCCURRENCES = 3) :
- Passe 1 : comptage des occurrences brutes par position arrondie à 100m
- Ne garder que les positions avec >= MIN_OCCURRENCES occurrences
  → élimine automatiquement cars scolaires et lignes interurbaines peu fréquentes

Filtre 2 — Distance adaptative (rayon de Voronoi) :
- Chaque arrêt est assigné à la commune au centroïde le plus proche (KDTree)
- Mais : un arrêt à 4km du centroïde d'une petite commune peut être sur une commune voisine
- Solution : pour chaque commune, max_dist = 0.5 × distance au voisin le plus proche
  (= rayon de Voronoi approximatif), plafonné entre 1km et 5km
- Un arrêt assigné à une commune mais trop loin de son centroïde est rejeté
  → élimine les misassignations aux frontières (petites communes péri-urbaines)

Score TC = percentile du nb d'arrêts qualifiés par commune
Score transport final = 0.5 × score_gare + 0.5 × score_TC

Ce script doit être lancé APRÈS import_transports.py (qui calcule distance_gare_km).
"""

import asyncio
import sys
import os
import tempfile
import numpy as np
import pandas as pd
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
from scipy.spatial import cKDTree
from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score


# France métropolitaine (hors DOM-TOM)
LAT_MIN, LAT_MAX = 41.0, 52.0
LON_MIN, LON_MAX = -6.0, 10.0

CHUNK_SIZE = 300_000

# Seuil minimum d'occurrences pour qu'un arrêt soit considéré "qualifié"
# Un arrêt scolaire/hebdomadaire apparaît 1-2× dans le dataset consolidé.
# Un arrêt régulier apparaît dans plusieurs feeds d'opérateurs : ≥ 3 occurrences.
MIN_OCCURRENCES = 3


async def get_tc_url() -> str:
    """
    Récupère dynamiquement l'URL du CSV national des arrêts TC depuis data.gouv.fr.
    Fallback sur une URL hardcodée si l'API est indisponible.
    """
    FALLBACK_URL = (
        "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com"
        "/81333/81333.20260113.121610.549772.csv"
    )
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://www.data.gouv.fr/api/1/datasets/arrets-de-transport-en-france/"
            )
            if resp.status_code == 200:
                data = resp.json()
                resources = data.get("resources", [])
                # Prendre le premier fichier CSV (le plus récent en premier)
                for res in resources:
                    if res.get("format", "").lower() == "csv" and res.get("url"):
                        url = res["url"]
                        print(f"  → URL dynamique : {res.get('title', '')} ({res.get('latest', url)[:80]})")
                        return res.get("latest") or url
    except Exception as e:
        print(f"  → API data.gouv.fr indisponible ({e}), utilisation URL fallback")
    print(f"  → URL fallback : {FALLBACK_URL[:80]}...")
    return FALLBACK_URL


async def telecharger_arrets(tmp_path: str, url: str):
    """Télécharge le fichier national des arrêts vers un fichier temporaire."""
    print(f"Téléchargement arrêts TC nationaux (~437 Mo)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            total = 0
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=4 * 1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
                    if total % (50 * 1024 * 1024) == 0:
                        print(f"  → {total // (1024*1024)} Mo téléchargés...")
    print(f"  → Téléchargement terminé ({total // (1024*1024)} Mo)")


def compter_arrets_par_commune(
    tmp_path: str,
    codes: list,
    comm_lats: np.ndarray,
    comm_lons: np.ndarray,
    min_occurrences: int = MIN_OCCURRENCES,
) -> dict:
    """
    Deux passes sur le CSV :
    1. Comptage des occurrences brutes par position (rlat, rlon)
    2. Assignation aux communes des seules positions avec >= min_occurrences occurrences

    Retourne un dict {code_insee: nb_arrets_qualifies}.
    """
    # KDTree sur coordonnées converties en (x, y, z) cartésien
    R = 1.0
    xs = R * np.cos(np.radians(comm_lats)) * np.cos(np.radians(comm_lons))
    ys = R * np.cos(np.radians(comm_lats)) * np.sin(np.radians(comm_lons))
    zs = R * np.sin(np.radians(comm_lats))
    tree = cKDTree(np.column_stack([xs, ys, zs]))
    print(f"  → KDTree construit sur {len(codes)} communes")

    # Rayon de Voronoi adaptatif par commune :
    # max_dist = 0.5 × distance au voisin le plus proche, plafonné entre 1km et 5km
    # → rejette les arrêts sur la commune voisine assignés par erreur au centroïde le plus proche
    self_dists, _ = tree.query(np.column_stack([xs, ys, zs]), k=2)
    nn_dists_sphere = self_dists[:, 1]  # distance au 2e plus proche = voisin immédiat
    nn_dists_km = nn_dists_sphere * 6371.0
    max_dists_km = np.clip(nn_dists_km * 0.5, 1.0, 5.0)
    max_dists_sphere = max_dists_km / 6371.0
    print(f"  → Rayon Voronoi : médiane={np.median(max_dists_km):.1f}km, "
          f"min={max_dists_km.min():.1f}km, max={max_dists_km.max():.1f}km")

    csv_params = dict(
        chunksize=CHUNK_SIZE,
        usecols=["stop_lat", "stop_lon"],
        dtype={"stop_lat": float, "stop_lon": float},
        on_bad_lines="skip",
        low_memory=False,
    )

    # ── Passe 1 : comptage occurrences par position ───────────────────────────
    print(f"\nPasse 1/2 : comptage des occurrences par position...")
    position_counts: Counter = Counter()
    total_lignes = 0

    for chunk in pd.read_csv(tmp_path, **csv_params):
        total_lignes += len(chunk)
        chunk = chunk[
            (chunk["stop_lat"] >= LAT_MIN) & (chunk["stop_lat"] <= LAT_MAX) &
            (chunk["stop_lon"] >= LON_MIN) & (chunk["stop_lon"] <= LON_MAX)
        ]
        if chunk.empty:
            continue
        chunk = chunk.copy()
        chunk["rlat"] = chunk["stop_lat"].round(3)
        chunk["rlon"] = chunk["stop_lon"].round(3)
        # groupby pour compter les occurrences de chaque position dans ce chunk
        for (rlat, rlon), cnt in chunk.groupby(["rlat", "rlon"]).size().items():
            position_counts[(rlat, rlon)] += cnt

        if total_lignes % (CHUNK_SIZE * 5) == 0:
            print(f"  → {total_lignes:,} lignes lues...")

    n_unique = len(position_counts)
    counts_arr = np.array(list(position_counts.values()))
    n_qualifies = int((counts_arr >= min_occurrences).sum())

    print(f"  → {total_lignes:,} lignes traitées")
    print(f"  → {n_unique:,} positions uniques trouvées")
    print(f"  → Distribution : médiane={np.median(counts_arr):.0f}, "
          f"p75={np.percentile(counts_arr,75):.0f}, "
          f"p90={np.percentile(counts_arr,90):.0f}, "
          f"max={counts_arr.max()}")
    print(f"  → Seuil MIN_OCCURRENCES={min_occurrences} : "
          f"{n_qualifies:,} positions retenues / {n_unique:,} "
          f"({100*n_qualifies/n_unique:.1f}%)")

    # Ensemble des positions qualifiées (lookup O(1))
    qualifying: set = {pos for pos, cnt in position_counts.items() if cnt >= min_occurrences}

    # ── Passe 2 : assignation aux communes ───────────────────────────────────
    # Préparer un DataFrame des positions qualifiées pour le merge vectorisé
    qualifying_df = pd.DataFrame(list(qualifying), columns=["rlat", "rlon"])

    print(f"\nPasse 2/2 : assignation des arrêts qualifiés aux communes...")
    commune_stops: dict[int, set] = {}
    total_assignes = 0

    for chunk in pd.read_csv(tmp_path, **csv_params):
        chunk = chunk[
            (chunk["stop_lat"] >= LAT_MIN) & (chunk["stop_lat"] <= LAT_MAX) &
            (chunk["stop_lon"] >= LON_MIN) & (chunk["stop_lon"] <= LON_MAX)
        ]
        if chunk.empty:
            continue
        chunk = chunk.copy()
        chunk["rlat"] = chunk["stop_lat"].round(3)
        chunk["rlon"] = chunk["stop_lon"].round(3)

        # Déduplication dans le chunk + filtre vectorisé via merge
        chunk = chunk.drop_duplicates(subset=["rlat", "rlon"])
        chunk = chunk.merge(qualifying_df, on=["rlat", "rlon"], how="inner")
        if chunk.empty:
            continue

        # Requête KDTree
        rlat_r = np.radians(chunk["rlat"].values)
        rlon_r = np.radians(chunk["rlon"].values)
        xs_q = R * np.cos(rlat_r) * np.cos(rlon_r)
        ys_q = R * np.cos(rlat_r) * np.sin(rlon_r)
        zs_q = R * np.sin(rlat_r)
        dists, idxs = tree.query(np.column_stack([xs_q, ys_q, zs_q]))

        n_chunk_assigned = 0
        for i, (rlat, rlon) in enumerate(zip(chunk["rlat"].values, chunk["rlon"].values)):
            comm_idx = idxs[i]
            # Filtre Voronoi : rejeter si l'arrêt est trop loin du centroïde de la commune assignée
            if dists[i] <= max_dists_sphere[comm_idx]:
                if comm_idx not in commune_stops:
                    commune_stops[comm_idx] = set()
                commune_stops[comm_idx].add((rlat, rlon))
                n_chunk_assigned += 1
        total_assignes += n_chunk_assigned

    # Compter par commune
    result = {}
    for idx, stops in commune_stops.items():
        result[codes[idx]] = len(stops)

    print(f"  → {total_assignes:,} arrêts qualifiés assignés à {len(result):,} communes")
    return result


async def run():
    print("=== Import arrêts TC avec filtre fréquence (transport.data.gouv.fr) ===\n")
    await init_db()

    # 1. Récupérer l'URL dynamiquement
    tc_url = await get_tc_url()

    # 2. Charger les communes
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

    # 3. Télécharger vers fichier temp
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, prefix="tc_arrets_") as tmp:
        tmp_path = tmp.name

    try:
        await telecharger_arrets(tmp_path, tc_url)

        # 4. Compter les arrêts qualifiés par commune (2 passes)
        nb_arrets = compter_arrets_par_commune(tmp_path, codes, comm_lats, comm_lons)

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
            print("\n  → Fichier temporaire supprimé")

    # Stats finales
    counts = np.array([nb_arrets.get(c, 0) for c in codes])
    communes_avec_tc = (counts > 0).sum()
    print(f"\n  Communes avec >=1 arrêt qualifié : {communes_avec_tc:,} / {len(codes):,}")
    print(f"  Médiane (avec TC) : {np.median(counts[counts > 0]):.0f} arrêts")
    print(f"  P90 : {np.percentile(counts, 90):.0f}, max : {np.max(counts):.0f}")

    # 5. Sauvegarder nb_arrets_tc
    print("\nSauvegarde nb_arrets_tc...")
    async with async_session() as session:
        for i in range(0, len(codes), 5000):
            batch = codes[i:i + 5000]
            for code in batch:
                await session.execute(text(
                    "UPDATE scores SET nb_arrets_tc = :n WHERE code_insee = :c"
                ), {"n": nb_arrets.get(code, 0), "c": code})
            await session.commit()
            print(f"  → {min(i + 5000, len(codes))}/{len(codes)}")

    # 6. Recalculer score_transports = 0.5 * score_gare + 0.5 * score_TC
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

    async with async_session() as session:
        for i in range(0, len(codes_s), 5000):
            for j, code in enumerate(codes_s[i:i + 5000]):
                s = float(final_scores[i + j])
                if s >= 0:
                    await session.execute(text(
                        "UPDATE scores SET score_transports = :s WHERE code_insee = :c"
                    ), {"s": s, "c": code})
            await session.commit()
            print(f"  → {min(i + 5000, len(codes_s))}/{len(codes_s)} scores transports sauvegardés")

    # 7. Recalcul scores globaux
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
