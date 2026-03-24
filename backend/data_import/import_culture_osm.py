"""
Import des équipements culturels et commerciaux depuis OpenStreetMap (Overpass API).
Cinémas, bibliothèques, théâtres, musées, boulangeries, supermarchés par commune ET par IRIS.

Méthode :
  - 96 requêtes Overpass (1 par département métropolitain)
  - Amenity (culture) + Shop (commerce) en une seule requête par département
  - Attribution commune : POI → centroïde < 15 km → scores.poi_detail
  - Attribution IRIS : POI → centroïde IRIS < 2 km → iris_scores.poi_detail

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_culture_osm

Note : ~5–10 min selon la charge du serveur Overpass.
"""

import asyncio
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx

from sqlalchemy import text
from backend.database import async_session, init_db


OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Départements métropolitains (01-95 sauf 20 → 2A et 2B)
DEPARTEMENTS = (
    [f"{i:02d}" for i in range(1, 20)]
    + ["2A", "2B"]
    + [f"{i:02d}" for i in range(21, 96)]
)

# Amenity OSM → label POI
AMENITY_MAP = {
    "cinema":      "cinéma",
    "theatre":     "théâtre",
    "library":     "bibliothèque",
    "museum":      "musée",
    "arts_centre": "théâtre",   # salles polyvalentes, maisons de la culture
}

# Shop OSM → label POI
SHOP_MAP = {
    "bakery":      "boulangerie",
    "supermarket": "supermarché",
    "butcher":     "boucherie",
}

MAX_DIST_KM = 15.0    # distance max commune centroïde ↔ POI
MAX_DIST_IRIS_KM = 2.0  # distance max IRIS centroïde ↔ POI


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Distance haversine en km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def attribuer_nearest(
    poi_lat: float,
    poi_lng: float,
    zones: list[tuple[str, float, float]],
    max_km: float,
    box_deg: float,
) -> str | None:
    """Retourne le code de la zone la plus proche du POI dans la limite de max_km."""
    best_code = None
    best_dist = float("inf")
    for code, clat, clng in zones:
        if abs(clat - poi_lat) > box_deg or abs(clng - poi_lng) > box_deg * 1.5:
            continue
        dist = haversine_km(poi_lat, poi_lng, clat, clng)
        if dist < best_dist:
            best_dist = dist
            best_code = code
    return best_code if best_dist <= max_km else None


def build_overpass_query(dept: str) -> str:
    """Construit la requête Overpass amenity + shop + tourism pour un département."""
    amenities = "|".join(k for k in AMENITY_MAP if k != "museum")
    shops = "|".join(SHOP_MAP.keys())
    return f"""[out:json][timeout:60];
area["ref:INSEE"="{dept}"]["boundary"="administrative"]["admin_level"~"^[456]$"]->.dep;
(
  node["amenity"~"^({amenities})$"](area.dep);
  way["amenity"~"^({amenities})$"](area.dep);
  node["shop"~"^({shops})$"](area.dep);
  way["shop"~"^({shops})$"](area.dep);
  node["tourism"="museum"](area.dep);
  way["tourism"="museum"](area.dep);
);
out center qt;"""


async def requeter_departement(
    client: httpx.AsyncClient,
    dept: str,
) -> list[dict]:
    """Requête Overpass pour un département, avec retry en cas d'erreur 429/503."""
    query = build_overpass_query(dept)
    for tentative in range(3):
        try:
            resp = await client.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=90,
            )
            if resp.status_code == 429 or resp.status_code == 503:
                wait = 30 * (tentative + 1)
                print(f"  [{dept}] Rate limit ({resp.status_code}), attente {wait}s...")
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data.get("elements", [])
        except httpx.TimeoutException:
            print(f"  [{dept}] Timeout, tentative {tentative + 1}/3...")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"  [{dept}] Erreur : {e}")
            await asyncio.sleep(5)
    print(f"  [{dept}] Échec après 3 tentatives")
    return []


async def run():
    print("=== Import OSM Culture + Commerce (Overpass API) ===\n")
    await init_db()

    # Migrations
    async with async_session() as session:
        for ddl in [
            "ALTER TABLE scores ADD COLUMN poi_detail TEXT",
            "ALTER TABLE iris_scores ADD COLUMN poi_detail TEXT",
        ]:
            try:
                await session.execute(text(ddl))
                await session.commit()
            except Exception:
                pass

    # Charger communes (pour attribution commune) et IRIS (pour attribution quartier)
    print("Chargement communes + IRIS depuis la DB...")
    async with async_session() as session:
        r = await session.execute(
            text(
                "SELECT c.code_insee, c.latitude, c.longitude "
                "FROM communes c JOIN scores s ON c.code_insee = s.code_insee "
                "WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL"
            )
        )
        communes_rows = r.fetchall()

        r2 = await session.execute(
            text("SELECT iz.code_iris, iz.latitude, iz.longitude FROM iris_zones iz "
                 "WHERE iz.latitude IS NOT NULL AND iz.longitude IS NOT NULL AND iz.typ_iris != 'Z'")
        )
        iris_rows = r2.fetchall()

    # Index par département
    communes_par_dept: dict[str, list[tuple[str, float, float]]] = {}
    for code, lat, lng in communes_rows:
        dept = code[:2] if code[:2] != "97" else code[:3]
        if dept == "20":
            dept = "2A" if code < "20200" else "2B"
        communes_par_dept.setdefault(dept, []).append((code, lat, lng))

    iris_par_dept: dict[str, list[tuple[str, float, float]]] = {}
    for code_iris, lat, lng in iris_rows:
        dept = code_iris[:2] if code_iris[:2] != "97" else code_iris[:3]
        if dept == "20":
            dept = "2A" if code_iris < "20200" else "2B"
        iris_par_dept.setdefault(dept, []).append((code_iris, lat, lng))

    print(f"  → {len(communes_rows):,} communes, {len(iris_rows):,} IRIS chargés\n")

    # Requêtes Overpass département par département
    poi_communes: dict[str, dict] = {}
    poi_iris: dict[str, dict] = {}
    nb_pois_total = 0

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for i, dept in enumerate(DEPARTEMENTS, 1):
            communes_dept = communes_par_dept.get(dept, [])
            iris_dept = iris_par_dept.get(dept, [])
            if not communes_dept:
                continue

            elements = await requeter_departement(client, dept)
            nb_pois = 0

            for el in elements:
                if el["type"] == "node":
                    lat, lng = el.get("lat"), el.get("lon")
                elif el["type"] == "way" and "center" in el:
                    lat, lng = el["center"]["lat"], el["center"]["lon"]
                else:
                    continue
                if lat is None or lng is None:
                    continue

                tags = el.get("tags", {})
                label = (AMENITY_MAP.get(tags.get("amenity", ""))
                         or SHOP_MAP.get(tags.get("shop", ""))
                         or ("musée" if tags.get("tourism") == "museum" else None))
                if not label:
                    continue

                # Attribution commune
                code = attribuer_nearest(lat, lng, communes_dept, MAX_DIST_KM, 0.15)
                if code:
                    if code not in poi_communes:
                        poi_communes[code] = {}
                    poi_communes[code][label] = poi_communes[code].get(label, 0) + 1
                    nb_pois += 1

                # Attribution IRIS
                if iris_dept:
                    code_iris = attribuer_nearest(lat, lng, iris_dept, MAX_DIST_IRIS_KM, 0.02)
                    if code_iris:
                        if code_iris not in poi_iris:
                            poi_iris[code_iris] = {}
                        poi_iris[code_iris][label] = 1  # présence seulement

            nb_pois_total += nb_pois
            print(f"  [{i:02d}/{len(DEPARTEMENTS)}] Dép. {dept} : {nb_pois} POIs → {len(poi_communes)} communes, {len(poi_iris)} IRIS")

            await asyncio.sleep(1.5)

    print(f"\n  → {nb_pois_total:,} POIs traités au total")

    # Merge communes
    print("\nMise à jour scores.poi_detail...")
    async with async_session() as session:
        result = await session.execute(text("SELECT code_insee, poi_detail FROM scores"))
        existing = {r[0]: (json.loads(r[1]) if r[1] else {}) for r in result.fetchall()}
        nb_maj = 0
        for code, new_data in poi_communes.items():
            if code not in existing:
                continue
            merged = dict(existing[code])
            merged.update(new_data)
            await session.execute(
                text("UPDATE scores SET poi_detail = :d, updated_at = CURRENT_TIMESTAMP WHERE code_insee = :c"),
                {"d": json.dumps(merged, ensure_ascii=False), "c": code},
            )
            nb_maj += 1
            if nb_maj % 2000 == 0:
                await session.commit()
        await session.commit()
    print(f"  → {nb_maj:,} communes mises à jour")

    # Merge IRIS
    print("Mise à jour iris_scores.poi_detail...")
    async with async_session() as session:
        result = await session.execute(text("SELECT code_iris, poi_detail FROM iris_scores"))
        existing_iris = {r[0]: (json.loads(r[1]) if r[1] else {}) for r in result.fetchall()}
        nb_iris = 0
        for code_iris, new_data in poi_iris.items():
            if code_iris not in existing_iris:
                continue
            merged = dict(existing_iris[code_iris])
            merged.update(new_data)
            await session.execute(
                text("UPDATE iris_scores SET poi_detail = :d, updated_at = CURRENT_TIMESTAMP WHERE code_iris = :c"),
                {"d": json.dumps(merged, ensure_ascii=False), "c": code_iris},
            )
            nb_iris += 1
            if nb_iris % 5000 == 0:
                await session.commit()
        await session.commit()
    print(f"  → {nb_iris:,} IRIS mis à jour")
    print("\n=== Import OSM Culture + Commerce terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
