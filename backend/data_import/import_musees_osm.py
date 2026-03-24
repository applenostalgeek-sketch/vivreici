"""
Import ciblé des musées OSM (tourism=museum) pour tous les départements.
À lancer une seule fois pour combler le manque dû au mauvais tag (amenity vs tourism).

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_musees_osm
"""

import asyncio
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx

from sqlalchemy import text
from backend.database import async_session, init_db


OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.karte.io/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

DEPARTEMENTS = (
    [f"{i:02d}" for i in range(1, 20)]
    + ["2A", "2B"]
    + [f"{i:02d}" for i in range(21, 96)]
)

MAX_DIST_KM = 15.0
MAX_DIST_IRIS_KM = 2.0


def haversine_km(lat1, lng1, lat2, lng2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def nearest(poi_lat, poi_lng, zones, max_km, box_deg):
    best_code, best_dist = None, float("inf")
    for code, clat, clng in zones:
        if abs(clat - poi_lat) > box_deg or abs(clng - poi_lng) > box_deg * 1.5:
            continue
        d = haversine_km(poi_lat, poi_lng, clat, clng)
        if d < best_dist:
            best_dist, best_code = d, code
    return best_code if best_dist <= max_km else None


async def query_museums(client, dept):
    q = f"""[out:json][timeout:60];
area["ref:INSEE"="{dept}"]["boundary"="administrative"]["admin_level"~"^[456]$"]->.dep;
(node["tourism"="museum"](area.dep);way["tourism"="museum"](area.dep););
out center qt;"""
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(3):
            try:
                resp = await client.post(endpoint, data={"data": q}, timeout=90)
                if resp.status_code in (429, 503):
                    await asyncio.sleep(30 * (attempt + 1))
                    continue
                if resp.status_code == 504:
                    await asyncio.sleep(15)
                    continue
                resp.raise_for_status()
                return resp.json().get("elements", [])
            except httpx.TimeoutException:
                await asyncio.sleep(10)
            except Exception as e:
                print(f"    [{dept}] {endpoint}: {e}")
                await asyncio.sleep(5)
        print(f"    [{dept}] {endpoint} épuisé")
        await asyncio.sleep(20)
    return []


async def run():
    print("=== Import musées OSM (tourism=museum) ===\n")
    await init_db()

    # Charger communes + IRIS
    async with async_session() as session:
        r = await session.execute(text(
            "SELECT c.code_insee, c.latitude, c.longitude "
            "FROM communes c JOIN scores s ON c.code_insee = s.code_insee "
            "WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL"
        ))
        communes_rows = r.fetchall()
        r2 = await session.execute(text(
            "SELECT iz.code_iris, iz.latitude, iz.longitude FROM iris_zones iz "
            "WHERE iz.latitude IS NOT NULL AND iz.longitude IS NOT NULL AND iz.typ_iris != 'Z'"
        ))
        iris_rows = r2.fetchall()

    communes_par_dept: dict = {}
    for code, lat, lng in communes_rows:
        dept = code[:2] if code[:2] != "97" else code[:3]
        if dept == "20":
            dept = "2A" if code < "20200" else "2B"
        communes_par_dept.setdefault(dept, []).append((code, lat, lng))

    iris_par_dept: dict = {}
    for code_iris, lat, lng in iris_rows:
        dept = code_iris[:2] if code_iris[:2] != "97" else code_iris[:3]
        if dept == "20":
            dept = "2A" if code_iris < "20200" else "2B"
        iris_par_dept.setdefault(dept, []).append((code_iris, lat, lng))

    poi_communes: dict = {}
    poi_iris: dict = {}
    nb_total = 0

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for i, dept in enumerate(DEPARTEMENTS, 1):
            communes_dept = communes_par_dept.get(dept, [])
            iris_dept = iris_par_dept.get(dept, [])
            if not communes_dept:
                continue

            elements = await query_museums(client, dept)
            nb = 0
            for el in elements:
                if el["type"] == "node":
                    lat, lng = el.get("lat"), el.get("lon")
                elif el["type"] == "way" and "center" in el:
                    lat, lng = el["center"]["lat"], el["center"]["lon"]
                else:
                    continue
                if lat is None or lng is None:
                    continue

                code = nearest(lat, lng, communes_dept, MAX_DIST_KM, 0.15)
                if code:
                    poi_communes.setdefault(code, {})["musée"] = poi_communes.get(code, {}).get("musée", 0) + 1
                    nb += 1

                if iris_dept:
                    code_iris = nearest(lat, lng, iris_dept, MAX_DIST_IRIS_KM, 0.02)
                    if code_iris:
                        poi_iris.setdefault(code_iris, {})["musée"] = 1

            nb_total += nb
            if nb > 0:
                print(f"  [{i:02d}/{len(DEPARTEMENTS)}] Dép. {dept} : {nb} musées")
            await asyncio.sleep(1.5)

    print(f"\n  → {nb_total:,} musées traités ({len(poi_communes)} communes, {len(poi_iris)} IRIS)")

    # Mise à jour communes
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
        await session.commit()
    print(f"  → {nb_maj:,} communes mises à jour")

    # Mise à jour IRIS
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
        await session.commit()
    print(f"  → {nb_iris:,} IRIS mis à jour")
    print("\n=== Import musées OSM terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
