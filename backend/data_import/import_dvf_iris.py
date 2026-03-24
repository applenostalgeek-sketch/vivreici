"""
Scoring IRIS de l'immobilier (DVF 2024 — data.gouv.fr geo-dvf).
Jointure spatiale : coordonnées GPS des transactions → zone IRIS via geopandas.
Source contours IRIS : archive IGN 2024 (même que import_iris_zones.py)

NOTE : téléchargement des 101 fichiers départementaux (~100 Mo total) +
       archive IRIS contours (~150 Mo). Script long (~10-15 min).
"""

import asyncio
import gzip
import io
import sys
import os
import tempfile

import httpx
import pandas as pd
import geopandas as gpd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import percentile_to_score, calculer_score_global
from backend.data_import.import_dvf import BASE_URL, DEPARTEMENTS, TYPES_LOGEMENT, MIN_TRANSACTIONS
from backend.data_import.import_iris_zones import IRIS_SHP_URL

MAX_CONCURRENT = 8

# Colonnes DVF nécessaires (avec lat/lon pour jointure spatiale)
COLONNES_IRIS = [
    "type_local", "valeur_fonciere", "surface_reelle_bati",
    "latitude", "longitude",
]


async def telecharger_departement(client, sem, dep) -> pd.DataFrame | None:
    url = f"{BASE_URL}{dep}.csv.gz"
    async with sem:
        try:
            resp = await client.get(url, timeout=60)
            resp.raise_for_status()
            content = gzip.decompress(resp.content)
            df = pd.read_csv(
                io.BytesIO(content), sep=",", low_memory=False,
                usecols=lambda c: c in set(COLONNES_IRIS),
            )
            return df
        except Exception:
            return None


async def telecharger_dvf_gps() -> pd.DataFrame:
    """Télécharge tous les départements DVF avec coordonnées GPS."""
    print("Téléchargement DVF 2023 (geo-dvf, avec GPS)...")
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = [telecharger_departement(client, sem, dep) for dep in DEPARTEMENTS]
        results = await asyncio.gather(*tasks)

    frames = [r for r in results if r is not None and not r.empty]
    df = pd.concat(frames, ignore_index=True)
    print(f"  → {len(df):,} transactions brutes")
    return df


def filtrer_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Garde uniquement les ventes résidentielles avec GPS valides."""
    df = df[df["type_local"].isin(TYPES_LOGEMENT)].copy()
    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati", "latitude", "longitude"])

    for col in ["valeur_fonciere", "surface_reelle_bati", "latitude", "longitude"]:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "."), errors="coerce"
        )

    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati", "latitude", "longitude"])
    df = df[df["surface_reelle_bati"] > 9]
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    df = df[(df["prix_m2"] > 200) & (df["prix_m2"] < 50000)]

    print(f"  → {len(df):,} transactions valides avec coordonnées GPS")
    return df[["latitude", "longitude", "prix_m2"]].copy()


async def charger_contours_iris() -> gpd.GeoDataFrame:
    """Télécharge les contours IRIS (Lambert93) pour la jointure spatiale."""
    import py7zr

    print("Téléchargement contours IRIS 2024 pour jointure spatiale...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        resp = await client.get(IRIS_SHP_URL)
        resp.raise_for_status()

    print(f"  → Reçu ({len(resp.content) / 1024 / 1024:.0f} Mo). Extraction shapefile...")

    with tempfile.TemporaryDirectory() as tmpdir:
        with py7zr.SevenZipFile(io.BytesIO(resp.content), mode="r") as z:
            z.extractall(path=tmpdir)

        shp_files = []
        for root, _, files in os.walk(tmpdir):
            for f in files:
                if f.endswith(".shp"):
                    shp_files.append(os.path.join(root, f))

        if not shp_files:
            raise FileNotFoundError("Aucun shapefile trouvé dans l'archive IRIS")

        gdf = gpd.read_file(shp_files[0])

    print(f"  → {len(gdf):,} zones IRIS (CRS: {gdf.crs})")

    # Identifier la colonne CODE_IRIS
    col_code = next(
        (c for c in gdf.columns if "CODE_IRIS" in c.upper()),
        None
    )
    if not col_code:
        raise ValueError(f"CODE_IRIS introuvable. Colonnes disponibles : {list(gdf.columns)}")

    return gdf[[col_code, "geometry"]].rename(columns={col_code: "code_iris"})


def joindre_iris(df_dvf: pd.DataFrame, gdf_iris: gpd.GeoDataFrame) -> pd.DataFrame:
    """Jointure spatiale : chaque transaction → zone IRIS contenant le point."""
    print(f"Jointure spatiale ({len(df_dvf):,} transactions × {len(gdf_iris):,} IRIS)...")

    gdf_pts = gpd.GeoDataFrame(
        df_dvf[["prix_m2"]].copy(),
        geometry=gpd.points_from_xy(df_dvf["longitude"], df_dvf["latitude"]),
        crs="EPSG:4326",
    )
    # Convertir en Lambert 93 pour correspondre aux contours IGN
    gdf_pts_l93 = gdf_pts.to_crs("EPSG:2154")

    joined = gpd.sjoin(gdf_pts_l93, gdf_iris, how="left", predicate="within")
    matched = joined["code_iris"].notna().sum()
    print(f"  → {matched:,} transactions géolocalisées dans un IRIS ({matched/len(df_dvf)*100:.0f}%)")

    return joined[["code_iris", "prix_m2"]].dropna(subset=["code_iris"])


async def run():
    await init_db()

    df_dvf = await telecharger_dvf_gps()
    df_dvf = filtrer_transactions(df_dvf)

    gdf_iris = await charger_contours_iris()

    df_joined = joindre_iris(df_dvf, gdf_iris)

    # Médiane par IRIS
    print("Calcul médiane par IRIS...")
    df_agg = (
        df_joined
        .groupby("code_iris")["prix_m2"]
        .agg(prix_m2_median="median", nb_transactions="count")
        .reset_index()
    )
    df_agg = df_agg[df_agg["nb_transactions"] >= MIN_TRANSACTIONS]
    print(f"  → {len(df_agg):,} IRIS avec >= {MIN_TRANSACTIONS} transactions")

    # Scoring percentile
    serie_prix = df_agg["prix_m2_median"]
    df_agg["score_immobilier"] = df_agg["prix_m2_median"].apply(
        lambda x: percentile_to_score(x, serie_prix, "inverse")
    )

    # Upsert
    print("Sauvegarde en base...")
    async with async_session() as session:
        count = 0
        for _, row in df_agg.iterrows():
            await session.execute(text("""
                INSERT INTO iris_scores (
                    code_iris, score_global, lettre,
                    score_equipements, score_sante, score_immobilier, score_revenus,
                    nb_equipements, nb_medecins_pour_10000,
                    prix_m2_median, revenu_median, taux_pauvrete,
                    nb_categories_scorees, updated_at
                ) VALUES (
                    :code, 50, 'C',
                    -1, -1, :simmo, -1,
                    0, 0, :prix, 0, 0,
                    1, CURRENT_TIMESTAMP
                )
                ON CONFLICT(code_iris) DO UPDATE SET
                    score_immobilier = excluded.score_immobilier,
                    prix_m2_median   = excluded.prix_m2_median,
                    updated_at       = excluded.updated_at
            """), {
                "code":  row["code_iris"],
                "simmo": float(row["score_immobilier"]),
                "prix":  float(row["prix_m2_median"]),
            })
            count += 1
            if count % 2000 == 0:
                await session.commit()
                print(f"  → {count}/{len(df_agg)}")

        await session.commit()

    print(f"  → {count} IRIS scorés immobilier.")
    await recalculer_scores_globaux()


async def recalculer_scores_globaux():
    """Recalcule score_global IRIS depuis tous les sous-scores disponibles."""
    from backend.scoring import calculer_score_global as calc

    print("Recalcul scores globaux IRIS...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_iris,
                   score_equipements, score_sante, score_immobilier, score_revenus
            FROM iris_scores
        """))
        rows = result.fetchall()

        nb = 0
        for row in rows:
            code, seq, sante, simmo, srev = row
            sous_scores = {}
            if seq   is not None and seq   >= 0: sous_scores["equipements"] = seq
            if sante is not None and sante >= 0: sous_scores["sante"]       = sante
            if simmo is not None and simmo >= 0: sous_scores["immobilier"]  = simmo
            if srev  is not None and srev  >= 0: sous_scores["revenus"]     = srev
            if not sous_scores:
                continue
            score, lettre, nb_cat = calc(sous_scores)
            await session.execute(text("""
                UPDATE iris_scores
                SET score_global = :sg, lettre = :l, nb_categories_scorees = :nb
                WHERE code_iris = :c
            """), {"sg": score, "l": lettre, "nb": nb_cat, "c": code})
            nb += 1
            if nb % 5000 == 0:
                await session.commit()

        await session.commit()
    print(f"  → {nb} scores globaux IRIS recalculés.")


if __name__ == "__main__":
    asyncio.run(run())
