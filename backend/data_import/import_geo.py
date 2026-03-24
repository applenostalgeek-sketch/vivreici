"""
Import des communes françaises depuis l'API Géo officielle.
https://geo.api.gouv.fr/communes
Pas de téléchargement nécessaire — API directe.
"""

import asyncio
import httpx
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.models import Commune


# Codes des départements métropolitains + DOM
DEPARTEMENTS = [
    "01","02","03","04","05","06","07","08","09","10",
    "11","12","13","14","15","16","17","18","19","2A","2B",
    "21","22","23","24","25","26","27","28","29","30",
    "31","32","33","34","35","36","37","38","39","40",
    "41","42","43","44","45","46","47","48","49","50",
    "51","52","53","54","55","56","57","58","59","60",
    "61","62","63","64","65","66","67","68","69","70",
    "71","72","73","74","75","76","77","78","79","80",
    "81","82","83","84","85","86","87","88","89","90",
    "91","92","93","94","95",
    "971","972","973","974","976",
]

# Mapping département → région (simplifié)
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


async def importer_departement(client: httpx.AsyncClient, dep: str, retries: int = 3) -> list[dict]:
    """Récupère toutes les communes d'un département via l'API Géo, avec retry."""
    url = (
        f"https://geo.api.gouv.fr/departements/{dep}/communes"
        f"?fields=nom,code,population,codesPostaux&format=json"
    )

    for attempt in range(retries):
        try:
            resp = await client.get(url, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < retries - 1:
                wait = 2.0 * (attempt + 1)
                await asyncio.sleep(wait)
            else:
                print(f"  ⚠️  Département {dep} ignoré après {retries} tentatives: {e}")
    return []


async def run():
    """Import principal : charge toutes les communes de France."""
    print("Import des communes françaises depuis l'API Géo...")
    await init_db()

    all_communes = []
    failed = []

    async with httpx.AsyncClient() as client:
        for i, dep in enumerate(DEPARTEMENTS):
            communes = await importer_departement(client, dep)
            if communes:
                all_communes.extend(communes)
            else:
                failed.append(dep)

            if (i + 1) % 10 == 0:
                print(f"  → {i+1}/{len(DEPARTEMENTS)} départements ({len(all_communes)} communes, {len(failed)} échecs)")

            await asyncio.sleep(1.0)  # 1 seconde entre chaque département

    if failed:
        print(f"  ⚠️  {len(failed)} département(s) ignoré(s) : {', '.join(failed)}")
    print(f"✅ {len(all_communes)} communes récupérées. Import en base (upsert)...")

    async with async_session() as session:

        batch_size = 500
        for i in range(0, len(all_communes), batch_size):
            batch = all_communes[i:i + batch_size]

            for c in batch:
                code = c.get("code", "")
                dep = code[:2] if len(code) >= 2 else ""
                # Corse
                if code.startswith("2A") or code.startswith("2B"):
                    dep = code[:2]
                elif len(code) >= 3 and code[:3] in ["971","972","973","974","976"]:
                    dep = code[:3]

                # Upsert sans écraser les coordonnées existantes
                await session.execute(
                    text("""
                        INSERT INTO communes (code_insee, nom, departement, region, population, codes_postaux, latitude, longitude, updated_at)
                        VALUES (:code, :nom, :dep, :region, :pop, :cp, NULL, NULL, CURRENT_TIMESTAMP)
                        ON CONFLICT(code_insee) DO UPDATE SET
                            nom=excluded.nom,
                            departement=excluded.departement,
                            region=excluded.region,
                            population=excluded.population,
                            codes_postaux=excluded.codes_postaux,
                            updated_at=excluded.updated_at
                    """),
                    {
                        "code": code,
                        "nom": c.get("nom", ""),
                        "dep": dep,
                        "region": DEP_TO_REGION.get(dep, "Autre"),
                        "pop": c.get("population", 0) or 0,
                        "cp": ",".join(c.get("codesPostaux", [])),
                    }
                )

            await session.commit()
            print(f"  → {min(i + batch_size, len(all_communes))}/{len(all_communes)} communes importées")

    print(f"Import terminé : {len(all_communes)} communes en base.")


if __name__ == "__main__":
    asyncio.run(run())
