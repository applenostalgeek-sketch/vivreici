"""
Import des zones IRIS depuis les Contours IRIS® IGN 2024.
Source : https://geoservices.ign.fr/contoursiris
~50 000 zones en France métropolitaine.

Populate la table iris_zones avec : code_iris, nom, code_commune, typ_iris,
population (0 par défaut), latitude, longitude (centroïde WGS84).
"""

import asyncio
import httpx
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db

# Contours IRIS® 2024 (Lambert 93) — IGN via data.geopf.fr
IRIS_SHP_URL = (
    "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/"
    "CONTOURS-IRIS_3-0__SHP_LAMB93_FXX_2024-01-01/"
    "CONTOURS-IRIS_3-0__SHP_LAMB93_FXX_2024-01-01.7z"
)


async def telecharger_contours() -> bytes:
    print("Téléchargement des contours IRIS 2024 (IGN, ~200 Mo)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        resp = await client.get(IRIS_SHP_URL)
        resp.raise_for_status()
    print(f"  → Reçu ({len(resp.content) / 1024 / 1024:.0f} Mo)")
    return resp.content


def extraire_centroides(data: bytes) -> list[dict]:
    """Extrait les centroides WGS84 depuis l'archive 7z contenant le shapefile."""
    import py7zr
    import geopandas as gpd

    print("  → Extraction de l'archive 7z...")
    with tempfile.TemporaryDirectory() as tmpdir:
        with py7zr.SevenZipFile(io.BytesIO(data), mode="r") as z:
            z.extractall(path=tmpdir)

        # Trouver le fichier .shp
        shp_files = []
        for root, dirs, files in os.walk(tmpdir):
            for f in files:
                if f.endswith(".shp"):
                    shp_files.append(os.path.join(root, f))

        if not shp_files:
            raise FileNotFoundError("Aucun fichier .shp trouvé dans l'archive")

        shp_path = shp_files[0]
        print(f"  → Lecture du shapefile : {os.path.basename(shp_path)}")

        gdf = gpd.read_file(shp_path)
        print(f"  → {len(gdf):,} zones IRIS chargées (CRS: {gdf.crs})")

        # Projeter en WGS84 pour les coordonnées
        # Calcul des centroides en Lambert 93 (projeté) puis conversion WGS84
        centroids_l93 = gdf.geometry.centroid
        gdf_centroids = gpd.GeoDataFrame(geometry=centroids_l93, crs=gdf.crs)
        gdf_centroids_wgs84 = gdf_centroids.to_crs("EPSG:4326")
        centroids = gdf_centroids_wgs84.geometry
        gdf_wgs84 = gdf  # not used for centroid anymore

        # Colonnes attendues dans le shapefile IGN IRIS
        # CODE_IRIS (9 chars), NOM_IRIS, INSEE_COM, TYP_IRIS
        col_map = {}
        for col in gdf.columns:
            lower = col.upper()
            if "CODE_IRIS" in lower or lower == "CODE_IRIS":
                col_map["code_iris"] = col
            elif "NOM_IRIS" in lower or lower == "NOM_IRIS":
                col_map["nom"] = col
            elif "INSEE_COM" in lower or lower == "INSEE_COM":
                col_map["code_commune"] = col
            elif "TYP_IRIS" in lower or lower == "TYP_IRIS":
                col_map["typ_iris"] = col

        print(f"  → Colonnes trouvées : {col_map}")
        if "code_iris" not in col_map:
            # Fallback : essayer d'autres noms courants
            for col in gdf.columns:
                print(f"    Colonne disponible : {col}")
            raise ValueError("Colonne CODE_IRIS introuvable — voir colonnes ci-dessus")

        zones = []
        for i, row in gdf.iterrows():
            centroid = centroids[i]
            zones.append({
                "code_iris":    str(row[col_map["code_iris"]]).strip(),
                "nom":          str(row.get(col_map.get("nom", ""), "")).strip()[:200],
                "code_commune": str(row[col_map["code_commune"]]).strip()[:5],
                "typ_iris":     str(row.get(col_map.get("typ_iris", ""), "H")).strip()[:1],
                "latitude":     round(centroid.y, 6),
                "longitude":    round(centroid.x, 6),
            })

    print(f"  → {len(zones):,} centroides extraits")
    return zones


async def run():
    await init_db()

    # Télécharger les contours
    data = await telecharger_contours()

    # Extraire les centroides
    zones = extraire_centroides(data)

    # Insérer en base
    print("Insertion des zones IRIS en base...")
    async with async_session() as session:
        # Vider la table existante pour réimport propre
        await session.execute(text("DELETE FROM iris_zones"))
        await session.commit()

        count = 0
        for z in zones:
            await session.execute(text("""
                INSERT INTO iris_zones (code_iris, nom, code_commune, typ_iris, population, latitude, longitude)
                VALUES (:code_iris, :nom, :code_commune, :typ_iris, 0, :lat, :lng)
                ON CONFLICT(code_iris) DO UPDATE SET
                    nom           = excluded.nom,
                    code_commune  = excluded.code_commune,
                    typ_iris      = excluded.typ_iris,
                    latitude      = excluded.latitude,
                    longitude     = excluded.longitude
            """), {
                "code_iris":    z["code_iris"],
                "nom":          z["nom"],
                "code_commune": z["code_commune"],
                "typ_iris":     z["typ_iris"],
                "lat":          z["latitude"],
                "lng":          z["longitude"],
            })
            count += 1
            if count % 5000 == 0:
                await session.commit()
                print(f"  → {count}/{len(zones)} zones insérées")

        await session.commit()

    print(f"\n✓ {count} zones IRIS importées.")


if __name__ == "__main__":
    asyncio.run(run())
