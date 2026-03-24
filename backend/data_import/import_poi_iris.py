"""
Import présence équipements par IRIS — sources GPS officielles.
Écoles (Annuaire éducation) + équipements sportifs (RES) → iris_scores.poi_detail.

Les sources OSM (culture + commerce) sont gérées par import_culture_osm.py.

Méthode :
  - Pour chaque POI GPS, trouve l'IRIS le plus proche via centroïde (< 2 km)
  - Utilise code_commune pour pré-filtrer → très rapide même sur 48 000 IRIS
  - Stocke présence uniquement (valeur = 1)

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_poi_iris
"""

import asyncio
import io
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
import pandas as pd

from sqlalchemy import text
from backend.database import async_session, init_db


EDU_URL = (
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets"
    "/fr-en-annuaire-education/exports/csv?limit=100000&lang=fr&timezone=Europe%2FParis"
)

RES_URL = (
    "https://equipements.sports.gouv.fr/api/explore/v2.1/catalog/datasets"
    "/data-es/exports/csv?limit=500000&lang=fr&timezone=Europe%2FParis"
)

MAX_DIST_IRIS_KM = 2.0

# Type établissement éducation → label
EDU_TYPE_MAP = {
    "Collège":              "collège",
    "Lycée":                "lycée",
    "Lycée polyvalent":     "lycée",
    "Lycée technologique":  "lycée",
    "EREA":                 "lycée",
    "Lycée professionnel":  "lycée_professionnel",
    "Lycée professionnel privé": "lycée_professionnel",
}

# Famille équipement sportif → label
RES_FAMILLE_MAP = {
    "Piscines":                    "piscine",
    "Piscine":                     "piscine",
    "Bassin de natation":          "piscine",
    "Gymnases - Halls":            "gymnase",
    "Gymnases":                    "gymnase",
    "Salles et terrains couverts": "gymnase",
    "Salle multisports":           "gymnase",
    "Salle non spécialisée":       "gymnase",
    "Football":                    "terrain_football",
    "Sports de ballon":            "terrain_football",
    "Stades":                      "stade",
    "Stades d'athlétisme":         "stade",
    "Terrains de grands jeux":     "terrain_football",
    "Terrains":                    "terrain_football",
}


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def trouver_iris(lat: float, lng: float, iris_commune: list[tuple[str, float, float]]) -> str | None:
    """Trouve l'IRIS le plus proche parmi ceux de la commune (< MAX_DIST_IRIS_KM)."""
    if not iris_commune:
        return None
    if len(iris_commune) == 1:
        # Commune avec un seul IRIS → attribution directe sans calcul
        code_iris, clat, clng = iris_commune[0]
        return code_iris if haversine_km(lat, lng, clat, clng) <= MAX_DIST_IRIS_KM else None
    best_code = None
    best_dist = float("inf")
    for code_iris, clat, clng in iris_commune:
        dist = haversine_km(lat, lng, clat, clng)
        if dist < best_dist:
            best_dist = dist
            best_code = code_iris
    return best_code if best_dist <= MAX_DIST_IRIS_KM else None


async def importer_education(iris_par_commune: dict) -> dict[str, dict]:
    """Télécharge l'annuaire éducation et assigne chaque école à son IRIS."""
    print("  → Téléchargement Annuaire éducation...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        resp = await client.get(EDU_URL)
        resp.raise_for_status()
    print(f"  → {len(resp.content) / 1024 / 1024:.1f} Mo reçus")

    df = pd.read_csv(io.BytesIO(resp.content), sep=";", dtype=str, low_memory=False)
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # Filtrer établissements ouverts
    if "etat" in df.columns:
        df = df[df["etat"].astype(str).str.strip() == "OUVERT"].copy()

    has_mat = "ecole_maternelle" in df.columns
    has_ele = "ecole_elementaire" in df.columns
    has_pro = "voie_professionnelle" in df.columns

    poi_iris: dict[str, dict] = {}
    nb_ok = 0

    for _, row in df.iterrows():
        # GPS
        try:
            lat = float(row.get("latitude", ""))
            lng = float(row.get("longitude", ""))
        except (ValueError, TypeError):
            continue

        # Code commune pour pré-filtrage IRIS
        code_commune = str(row.get("code_commune", "")).strip().zfill(5)
        if not code_commune or code_commune == "00000":
            continue

        # Déterminer le label
        typ = str(row.get("type_etablissement", "")).strip()
        label = EDU_TYPE_MAP.get(typ)
        if label is None and typ == "Ecole":
            is_mat = has_mat and str(row.get("ecole_maternelle", "")).strip() == "1"
            is_ele = has_ele and str(row.get("ecole_elementaire", "")).strip() == "1"
            if is_mat:
                label = "école_maternelle"
            elif is_ele:
                label = "école_primaire"
        if label is None:
            continue

        # Lycée général/technologique peut être pro si voie_professionnelle = 1
        if label == "lycée" and has_pro:
            if str(row.get("voie_professionnelle", "")).strip() == "1":
                label = "lycée_professionnel"

        iris_commune = iris_par_commune.get(code_commune, [])
        code_iris = trouver_iris(lat, lng, iris_commune)
        if not code_iris:
            continue

        if code_iris not in poi_iris:
            poi_iris[code_iris] = {}
        poi_iris[code_iris][label] = 1
        nb_ok += 1

    print(f"  → {nb_ok:,} établissements assignés à {len(poi_iris):,} IRIS")
    return poi_iris


async def importer_res(iris_par_commune: dict) -> dict[str, dict]:
    """Télécharge le RES et assigne chaque équipement sportif public à son IRIS."""
    print("  → Téléchargement RES (équipements sportifs)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        resp = await client.get(RES_URL)
        resp.raise_for_status()
    print(f"  → {len(resp.content) / 1024 / 1024:.1f} Mo reçus")

    df = pd.read_csv(io.BytesIO(resp.content), sep=";", dtype=str, low_memory=False)
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # Filtrer équipements publics uniquement
    if "equip_ouv_public_bool" in df.columns:
        df = df[df["equip_ouv_public_bool"].astype(str).str.lower() == "true"].copy()

    poi_iris: dict[str, dict] = {}
    nb_ok = 0
    familles_non_mappees = set()

    for _, row in df.iterrows():
        # GPS (equip_y = lat, equip_x = lon)
        try:
            lat = float(row.get("equip_y", ""))
            lng = float(row.get("equip_x", ""))
        except (ValueError, TypeError):
            continue

        code_commune = str(row.get("new_code", "")).strip().zfill(5)
        if not code_commune or code_commune == "00000":
            continue

        famille = str(row.get("equip_type_famille", "")).strip()
        label = RES_FAMILLE_MAP.get(famille)
        if label is None:
            famille_lower = famille.lower()
            for kw, lbl in [("piscine", "piscine"), ("gymnase", "gymnase"), ("hall", "gymnase"),
                             ("stade", "stade"), ("football", "terrain_football"), ("terrain", "terrain_football")]:
                if kw in famille_lower:
                    label = lbl
                    break
        if label is None:
            familles_non_mappees.add(famille)
            continue

        iris_commune = iris_par_commune.get(code_commune, [])
        code_iris = trouver_iris(lat, lng, iris_commune)
        if not code_iris:
            continue

        if code_iris not in poi_iris:
            poi_iris[code_iris] = {}
        poi_iris[code_iris][label] = 1
        nb_ok += 1

    if familles_non_mappees:
        print(f"  → Familles non mappées (ignorées) : {sorted(familles_non_mappees)[:5]}")
    print(f"  → {nb_ok:,} équipements assignés à {len(poi_iris):,} IRIS")
    return poi_iris


async def run():
    print("=== Import POI par IRIS (Éducation + RES) ===\n")
    await init_db()

    # Migration
    async with async_session() as session:
        try:
            await session.execute(text("ALTER TABLE iris_scores ADD COLUMN poi_detail TEXT"))
            await session.commit()
        except Exception:
            pass

    # Charger IRIS centroids groupés par commune
    print("Chargement IRIS depuis la DB...")
    async with async_session() as session:
        r = await session.execute(
            text("SELECT iz.code_iris, iz.code_commune, iz.latitude, iz.longitude "
                 "FROM iris_zones iz "
                 "WHERE iz.latitude IS NOT NULL AND iz.longitude IS NOT NULL AND iz.typ_iris != 'Z'")
        )
        rows = r.fetchall()

    iris_par_commune: dict[str, list[tuple[str, float, float]]] = {}
    for code_iris, code_commune, lat, lng in rows:
        iris_par_commune.setdefault(code_commune, []).append((code_iris, lat, lng))

    print(f"  → {len(rows):,} IRIS dans {len(iris_par_commune):,} communes\n")

    # Import éducation
    print("Import éducation...")
    poi_edu = await importer_education(iris_par_commune)

    # Import RES sports
    print("\nImport RES (sports)...")
    poi_res = await importer_res(iris_par_commune)

    # Fusionner les deux sources
    poi_merged: dict[str, dict] = {}
    for source in (poi_edu, poi_res):
        for code_iris, data in source.items():
            if code_iris not in poi_merged:
                poi_merged[code_iris] = {}
            poi_merged[code_iris].update(data)

    print(f"\n  → {len(poi_merged):,} IRIS avec données POI (éducation + sports)")

    # Écrire en base
    print("\nMise à jour iris_scores.poi_detail...")
    async with async_session() as session:
        r = await session.execute(text("SELECT code_iris, poi_detail FROM iris_scores"))
        existing = {r[0]: (json.loads(r[1]) if r[1] else {}) for r in r.fetchall()}

        nb_maj = 0
        for code_iris, new_data in poi_merged.items():
            if code_iris not in existing:
                continue
            merged = dict(existing[code_iris])
            merged.update(new_data)
            await session.execute(
                text("UPDATE iris_scores SET poi_detail = :d, updated_at = CURRENT_TIMESTAMP WHERE code_iris = :c"),
                {"d": json.dumps(merged, ensure_ascii=False), "c": code_iris},
            )
            nb_maj += 1
            if nb_maj % 5000 == 0:
                await session.commit()
                print(f"  → {nb_maj:,} IRIS mis à jour...")

        await session.commit()

    print(f"  → {nb_maj:,} IRIS mis à jour avec données POI")
    print("\n=== Import POI IRIS terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
