"""
Import des coordonnées GPS des communes françaises.
Source : france-geojson (GitHub, données IGN simplifiées)
https://github.com/gregoiredavid/france-geojson
"""

import asyncio
import httpx
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db


GEOJSON_URL = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes-version-simplifiee.geojson"


def compute_centroid(geometry: dict) -> tuple[float, float] | None:
    """Calcule le centroïde d'un polygone ou multipolygone."""
    try:
        if geometry["type"] == "Polygon":
            coords = geometry["coordinates"][0]
        elif geometry["type"] == "MultiPolygon":
            # Prendre le polygone le plus grand (premier anneau du premier polygone)
            coords = max(geometry["coordinates"], key=lambda p: len(p[0]))[0]
        else:
            return None

        lngs = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return sum(lats) / len(lats), sum(lngs) / len(lngs)
    except Exception:
        return None


async def run():
    """Télécharge le GeoJSON et met à jour les coordonnées en base."""
    await init_db()

    print("📥 Téléchargement du GeoJSON communes (france-geojson)...")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(GEOJSON_URL, timeout=120)
        resp.raise_for_status()
        data = resp.json()

    features = data.get("features", [])
    print(f"  → {len(features)} communes trouvées dans le GeoJSON")

    print("📍 Calcul des centroïdes et mise à jour en base...")

    async with async_session() as session:
        count = 0
        skipped = 0

        for i, feature in enumerate(features):
            code = feature.get("properties", {}).get("code", "")
            if not code:
                skipped += 1
                continue

            centroid = compute_centroid(feature.get("geometry", {}))
            if not centroid:
                skipped += 1
                continue

            lat, lng = centroid

            await session.execute(
                text("UPDATE communes SET latitude=:lat, longitude=:lng WHERE code_insee=:code"),
                {"lat": round(lat, 6), "lng": round(lng, 6), "code": code}
            )
            count += 1

            if (i + 1) % 5000 == 0:
                await session.commit()
                print(f"  → {i+1}/{len(features)} traités ({count} mis à jour)")

        await session.commit()

    print(f"✅ Coordonnées mises à jour : {count} communes ({skipped} ignorées).")


if __name__ == "__main__":
    asyncio.run(run())
