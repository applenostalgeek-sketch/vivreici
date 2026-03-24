"""
Import des contours polygon IRIS depuis les Contours IRIS® IGN 2024.
Même source que import_iris_zones.py (CONTOURS-IRIS shapefile IGN).
Stocke la géométrie GeoJSON simplifiée dans iris_zones.geometry.

Prérequis : geopandas, py7zr (déjà utilisés par import_iris_zones.py)
Durée estimée : ~5 min (téléchargement 200 Mo + projection + insertion)
"""

import asyncio
import httpx
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db

IRIS_SHP_URL = (
    "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/"
    "CONTOURS-IRIS_3-0__SHP_LAMB93_FXX_2024-01-01/"
    "CONTOURS-IRIS_3-0__SHP_LAMB93_FXX_2024-01-01.7z"
)


def simplifier_coords(coords, decimals=4):
    """Arrondit récursivement les coordonnées d'une géométrie GeoJSON."""
    if not coords:
        return coords
    if isinstance(coords[0], (int, float)):
        return [round(coords[0], decimals), round(coords[1], decimals)]
    return [simplifier_coords(c, decimals) for c in coords]


def simplifier_geometrie(geom_dict, decimals=4):
    """Simplifie les coordonnées d'un dict GeoJSON geometry."""
    return {**geom_dict, "coordinates": simplifier_coords(geom_dict["coordinates"], decimals)}


async def telecharger_contours() -> bytes:
    print("Téléchargement des contours IRIS 2024 (IGN, ~200 Mo)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        resp = await client.get(IRIS_SHP_URL)
        resp.raise_for_status()
    print(f"  → Reçu ({len(resp.content) / 1024 / 1024:.0f} Mo)")
    return resp.content


def extraire_geometries(data: bytes) -> list[dict]:
    """Extrait les polygones WGS84 simplifiés depuis l'archive 7z."""
    import py7zr
    import geopandas as gpd

    print("  → Extraction de l'archive 7z...")
    with tempfile.TemporaryDirectory() as tmpdir:
        with py7zr.SevenZipFile(io.BytesIO(data), mode="r") as z:
            z.extractall(path=tmpdir)

        shp_files = []
        for root, _, files in os.walk(tmpdir):
            for f in files:
                if f.endswith(".shp"):
                    shp_files.append(os.path.join(root, f))

        if not shp_files:
            raise FileNotFoundError("Aucun fichier .shp trouvé dans l'archive")

        shp_path = shp_files[0]
        print(f"  → Lecture du shapefile : {os.path.basename(shp_path)}")

        gdf = gpd.read_file(shp_path)
        print(f"  → {len(gdf):,} zones IRIS (CRS: {gdf.crs})")

        # Reprojeter en WGS84
        print("  → Reprojection WGS84...")
        gdf_wgs84 = gdf.to_crs("EPSG:4326")

        # Détecter la colonne code_iris
        col_code = None
        for col in gdf_wgs84.columns:
            if "CODE_IRIS" in col.upper():
                col_code = col
                break
        if not col_code:
            raise ValueError(f"Colonne CODE_IRIS introuvable. Colonnes : {list(gdf_wgs84.columns)}")

        print(f"  → Colonne code IRIS : {col_code}")

        zones = []
        for _, row in gdf_wgs84.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            try:
                geom_dict = geom.__geo_interface__
                geom_simple = simplifier_geometrie(geom_dict, decimals=4)
                zones.append({
                    "code_iris": str(row[col_code]).strip(),
                    "geometry": json.dumps(geom_simple, ensure_ascii=False, separators=(',', ':')),
                })
            except Exception as e:
                print(f"  ! Erreur géométrie {row[col_code]}: {e}")
                continue

    print(f"  → {len(zones):,} géométries extraites")
    return zones


async def run():
    await init_db()

    data = await telecharger_contours()

    print("Extraction des géométries...")
    zones = extraire_geometries(data)

    print("Sauvegarde en base (iris_zones.geometry)...")
    async with async_session() as session:
        # S'assurer que la colonne existe (idempotent)
        try:
            await session.execute(text("ALTER TABLE iris_zones ADD COLUMN geometry TEXT"))
            await session.commit()
        except Exception:
            pass  # Colonne déjà présente

        count = 0
        not_found = 0
        for z in zones:
            result = await session.execute(
                text("SELECT code_iris FROM iris_zones WHERE code_iris = :c"),
                {"c": z["code_iris"]}
            )
            if not result.fetchone():
                not_found += 1
                continue

            await session.execute(text("""
                UPDATE iris_zones SET geometry = :geom WHERE code_iris = :code
            """), {"geom": z["geometry"], "code": z["code_iris"]})
            count += 1

            if count % 5000 == 0:
                await session.commit()
                print(f"  → {count}/{len(zones)} géométries sauvegardées")

        await session.commit()

    print(f"\n✓ {count} géométries IRIS sauvegardées ({not_found} IRIS non trouvés en base).")
    print("  → Les polygones IRIS sont maintenant disponibles sur la carte.")


if __name__ == "__main__":
    asyncio.run(run())
