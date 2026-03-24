"""
Retry ciblé des départements OSM manqués.
À lancer après import_culture_osm.py pour compléter les départements en échec.

Utilise des endpoints Overpass alternatifs et des délais plus longs.

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_osm_retry
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


# Endpoints Overpass en rotation — si l'un est surchargé, on bascule sur l'autre
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

AMENITY_MAP = {
    "cinema":      "cinéma",
    "theatre":     "théâtre",
    "library":     "bibliothèque",
    "museum":      "musée",
    "arts_centre": "théâtre",
}

SHOP_MAP = {
    "bakery":      "boulangerie",
    "supermarket": "supermarché",
    "butcher":     "boucherie",
}

MAX_DIST_KM = 15.0
MAX_DIST_IRIS_KM = 2.0
PAUSE_BETWEEN_DEPTS = 4.0  # délai plus long que le script principal


def haversine_km(lat1, lng1, lat2, lng2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def attribuer_nearest(poi_lat, poi_lng, zones, max_km, box_deg):
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


def build_query(dept):
    amenities = "|".join(k for k in AMENITY_MAP if k != "museum")
    shops = "|".join(SHOP_MAP.keys())
    return f"""[out:json][timeout:90];
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


async def requeter_avec_fallback(client: httpx.AsyncClient, dept: str) -> list[dict]:
    """Essaie chaque endpoint Overpass jusqu'à succès."""
    query = build_query(dept)
    for endpoint in OVERPASS_ENDPOINTS:
        for tentative in range(3):
            try:
                resp = await client.post(endpoint, data={"data": query}, timeout=120)
                if resp.status_code in (429, 503):
                    wait = 45 * (tentative + 1)
                    print(f"    [{dept}] Rate limit sur {endpoint}, attente {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code == 504:
                    print(f"    [{dept}] 504 sur {endpoint}, tentative {tentative + 1}/3...")
                    await asyncio.sleep(20)
                    continue
                resp.raise_for_status()
                data = resp.json()
                return data.get("elements", [])
            except httpx.TimeoutException:
                print(f"    [{dept}] Timeout sur {endpoint}, tentative {tentative + 1}/3...")
                await asyncio.sleep(15)
            except Exception as e:
                print(f"    [{dept}] Erreur sur {endpoint}: {e}")
                await asyncio.sleep(10)
        print(f"    [{dept}] {endpoint} épuisé, bascule sur endpoint suivant...")
        await asyncio.sleep(30)

    print(f"    [{dept}] ÉCHEC sur tous les endpoints")
    return []


async def identifier_depts_manques(communes_par_dept: dict) -> list[str]:
    """
    Identifie les départements sans données OSM (culture/commerce).
    Un département est considéré manqué si ses communes n'ont ni cinéma ni boulangerie,
    sauf s'il est vraiment rural (< 50 communes avec poi_detail).
    """
    manques = []
    async with async_session() as session:
        for dept in DEPARTEMENTS:
            communes_dept = communes_par_dept.get(dept, [])
            if not communes_dept:
                continue
            codes = [c[0] for c in communes_dept]
            # Compter les communes du département qui ont culture ou commerce OSM
            placeholders = ",".join(f"'{c}'" for c in codes[:200])  # limite pour SQLite
            r = await session.execute(text(
                f"SELECT COUNT(*) FROM scores WHERE code_insee IN ({placeholders}) "
                f"AND (poi_detail LIKE '%cinéma%' OR poi_detail LIKE '%boulangerie%' "
                f"OR poi_detail LIKE '%bibliothèque%' OR poi_detail LIKE '%supermarché%')"
            ))
            nb_avec_osm = r.scalar()

            # Si 0 communes OSM dans un département avec > 3 communes, c'est manqué
            if nb_avec_osm == 0 and len(communes_dept) > 3:
                manques.append(dept)
                print(f"  Dép. {dept} manqué ({len(communes_dept)} communes, 0 OSM)")

    return manques


async def run():
    print("=== Retry OSM — Départements manqués ===\n")
    await init_db()

    # Charger communes + IRIS
    print("Chargement communes + IRIS...")
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

    # Identifier les départements manqués
    print("Identification des départements sans données OSM...")
    depts_manques = await identifier_depts_manques(communes_par_dept)

    if not depts_manques:
        print("Tous les départements ont des données OSM — rien à faire.")
        return

    print(f"\n{len(depts_manques)} départements à relancer : {depts_manques}\n")

    poi_communes: dict = {}
    poi_iris: dict = {}
    nb_pois_total = 0

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for i, dept in enumerate(depts_manques, 1):
            print(f"  [{i}/{len(depts_manques)}] Dép. {dept}...")
            communes_dept = communes_par_dept.get(dept, [])
            iris_dept = iris_par_dept.get(dept, [])

            elements = await requeter_avec_fallback(client, dept)
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

                code = attribuer_nearest(lat, lng, communes_dept, MAX_DIST_KM, 0.15)
                if code:
                    if code not in poi_communes:
                        poi_communes[code] = {}
                    poi_communes[code][label] = poi_communes[code].get(label, 0) + 1
                    nb_pois += 1

                if iris_dept:
                    code_iris = attribuer_nearest(lat, lng, iris_dept, MAX_DIST_IRIS_KM, 0.02)
                    if code_iris:
                        if code_iris not in poi_iris:
                            poi_iris[code_iris] = {}
                        poi_iris[code_iris][label] = 1

            nb_pois_total += nb_pois
            print(f"    → {nb_pois} POIs trouvés")

            if i < len(depts_manques):
                await asyncio.sleep(PAUSE_BETWEEN_DEPTS)

    print(f"\n  → {nb_pois_total:,} POIs traités")

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

    # Vérification finale
    print("\nVérification couverture finale...")
    await identifier_depts_manques(communes_par_dept)
    print("\n=== Retry OSM terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
