"""
Génère les contours polygon communes en dissolveant les polygones IRIS.
Utilise iris_zones.geometry (déjà importé) — aucun téléchargement nécessaire.

Prérequis : shapely (installé avec geopandas)
Durée estimée : ~2-3 min
"""

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db


def simplifier_coords(coords, decimals=3):
    """Arrondit récursivement les coordonnées GeoJSON."""
    if not coords:
        return coords
    if isinstance(coords[0], (int, float)):
        return [round(coords[0], decimals), round(coords[1], decimals)]
    return [simplifier_coords(c, decimals) for c in coords]


def simplifier_geometrie(geom_dict, decimals=3):
    return {**geom_dict, "coordinates": simplifier_coords(geom_dict["coordinates"], decimals)}


async def run():
    from shapely.ops import unary_union
    from shapely.geometry import shape

    await init_db()

    print("Lecture des géométries IRIS depuis la base...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_commune, geometry
            FROM iris_zones
            WHERE geometry IS NOT NULL
        """))
        rows = result.fetchall()

    print(f"  → {len(rows):,} zones IRIS avec géométrie")

    # Grouper par commune
    by_commune: dict[str, list] = {}
    for code_commune, geom_str in rows:
        by_commune.setdefault(code_commune, []).append(geom_str)

    print(f"  → {len(by_commune):,} communes à traiter")

    # Dissolve IRIS → commune
    print("Dissolution des polygones IRIS par commune...")
    commune_geoms = {}
    errors = 0
    for i, (code_commune, geom_strs) in enumerate(by_commune.items()):
        try:
            shapes = [shape(json.loads(g)) for g in geom_strs]
            dissolved = unary_union(shapes)
            geom_dict = dissolved.__geo_interface__
            geom_simple = simplifier_geometrie(geom_dict, decimals=3)
            commune_geoms[code_commune] = json.dumps(geom_simple, ensure_ascii=False, separators=(',', ':'))
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  ! Erreur commune {code_commune}: {e}")
        if (i + 1) % 5000 == 0:
            print(f"  → {i + 1}/{len(by_commune)} communes traitées")

    print(f"  → {len(commune_geoms):,} géométries calculées ({errors} erreurs)")

    # Sauvegarde
    print("Sauvegarde en base (communes.geometry)...")
    async with async_session() as session:
        try:
            await session.execute(text("ALTER TABLE communes ADD COLUMN geometry TEXT"))
            await session.commit()
        except Exception:
            pass

        count = 0
        not_found = 0
        for code, geom in commune_geoms.items():
            result = await session.execute(
                text("SELECT code_insee FROM communes WHERE code_insee = :c"),
                {"c": code}
            )
            if not result.fetchone():
                not_found += 1
                continue
            await session.execute(
                text("UPDATE communes SET geometry = :geom WHERE code_insee = :code"),
                {"geom": geom, "code": code}
            )
            count += 1
            if count % 5000 == 0:
                await session.commit()
                print(f"  → {count} communes sauvegardées")

        await session.commit()

    print(f"\n✓ {count} communes avec polygone ({not_found} codes non trouvés en base).")
    print("  → Les polygones communes sont disponibles sur la carte.")


if __name__ == "__main__":
    asyncio.run(run())
