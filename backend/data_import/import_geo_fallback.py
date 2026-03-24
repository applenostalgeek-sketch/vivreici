"""
Import des communes manquantes depuis le GeoJSON france-geojson.
Utilisé pour les départements que l'API Géo ne retourne pas (erreurs 500).
Le GeoJSON contient code INSEE + nom + géométrie pour toutes les communes françaises.
"""

import asyncio
import httpx
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db

GEOJSON_URL = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes-version-simplifiee.geojson"

DEP_TO_REGION = {
    **{d: "Île-de-France" for d in ["75","77","78","91","92","93","94","95"]},
    **{d: "Auvergne-Rhône-Alpes" for d in ["01","03","07","15","26","38","42","43","63","69","73","74"]},
    **{d: "Provence-Alpes-Côte d'Azur" for d in ["04","05","06","13","83","84"]},
    **{d: "Occitanie" for d in ["09","11","12","30","31","32","34","46","48","65","66","81","82"]},
    **{d: "Nouvelle-Aquitaine" for d in ["16","17","19","23","24","33","40","47","64","79","86","87"]},
    **{d: "Grand Est" for d in ["08","10","51","52","54","55","57","67","68","88","89"]},
    **{d: "Hauts-de-France" for d in ["02","59","60","62","80"]},
    **{d: "Bretagne" for d in ["22","29","35","56"]},
    **{d: "Normandie" for d in ["14","27","50","61","76"]},
    **{d: "Pays de la Loire" for d in ["44","49","53","72","85"]},
    **{d: "Centre-Val de Loire" for d in ["18","28","36","37","41","45"]},
    **{d: "Bourgogne-Franche-Comté" for d in ["21","25","39","58","70","71","89","90"]},
    **{d: "Corse" for d in ["2A","2B"]},
    **{d: "Outre-Mer" for d in ["971","972","973","974","976"]},
}


def compute_centroid(geometry: dict) -> tuple[float, float] | None:
    try:
        if geometry["type"] == "Polygon":
            coords = geometry["coordinates"][0]
        elif geometry["type"] == "MultiPolygon":
            coords = max(geometry["coordinates"], key=lambda p: len(p[0]))[0]
        else:
            return None
        lngs = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return round(sum(lats) / len(lats), 6), round(sum(lngs) / len(lngs), 6)
    except Exception:
        return None


async def run():
    await init_db()

    # Récupérer les codes INSEE déjà en base
    async with async_session() as session:
        result = await session.execute(text("SELECT code_insee FROM communes"))
        existing = {row[0] for row in result.fetchall()}

    print(f"Communes déjà en base : {len(existing)}")

    print("📥 Téléchargement du GeoJSON...")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(GEOJSON_URL, timeout=120)
        resp.raise_for_status()
        data = resp.json()

    features = data.get("features", [])
    manquantes = [f for f in features if f["properties"].get("code") not in existing]
    print(f"  → {len(manquantes)} communes manquantes à insérer")

    if not manquantes:
        print("✅ Aucune commune manquante.")
        return

    async with async_session() as session:
        count = 0
        for f in manquantes:
            code = f["properties"].get("code", "")
            nom = f["properties"].get("nom", "")
            if not code:
                continue

            dep = code[:2] if len(code) >= 2 else ""
            if code.startswith("2A") or code.startswith("2B"):
                dep = code[:2]
            elif len(code) >= 3 and code[:3] in ["971","972","973","974","976"]:
                dep = code[:3]

            centroid = compute_centroid(f.get("geometry", {}))
            lat, lng = centroid if centroid else (None, None)

            await session.execute(text("""
                INSERT INTO communes (code_insee, nom, departement, region, population, codes_postaux, latitude, longitude, updated_at)
                VALUES (:code, :nom, :dep, :region, 0, '', :lat, :lng, CURRENT_TIMESTAMP)
                ON CONFLICT(code_insee) DO UPDATE SET
                    latitude=COALESCE(excluded.latitude, communes.latitude),
                    longitude=COALESCE(excluded.longitude, communes.longitude)
            """), {
                "code": code, "nom": nom, "dep": dep,
                "region": DEP_TO_REGION.get(dep, "Autre"),
                "lat": lat, "lng": lng,
            })
            count += 1

            if count % 500 == 0:
                await session.commit()
                print(f"  → {count}/{len(manquantes)} insérées")

        await session.commit()

    print(f"✅ {count} communes manquantes insérées.")


if __name__ == "__main__":
    asyncio.run(run())
