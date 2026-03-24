"""
Import des établissements de santé depuis FINESS.
Pharmacies, hôpitaux, cliniques et cabinets médicaux par commune → poi_detail.

Source : FINESS — Fichier National des Établissements Sanitaires et Sociaux
Dataset : https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_finess
"""

import asyncio
import io
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
import pandas as pd

from sqlalchemy import text
from backend.database import async_session, init_db


FINESS_DATASET_API = "https://www.data.gouv.fr/api/1/datasets/53699569a3a729239d2046eb/"

# Mapping libcategorie (texte) → label POI
# Plus robuste que les codes numériques qui changent à chaque version FINESS
LIBCAT_MAP = [
    ("pharmacie",                      "pharmacie"),
    ("hôpital",                        "hôpital"),
    ("hospitalier",                    "hôpital"),
    ("soins chirurgicaux",             "clinique"),
    ("soins pluridisciplinaire",       "clinique"),
    ("soins médicaux",                 "clinique"),
    ("santé privé",                    "clinique"),
    ("laboratoire",                    "labo_analyse"),
    ("cabinet médical",                "cabinet_médical"),
    ("centre de santé",                "cabinet_médical"),
    ("maison de santé",                "cabinet_médical"),
]

# Colonnes du fichier FINESS (32 champs, pas de header)
# Ligne 0 = metadata (finess;etalab;96;YYYY-MM-DD) à ignorer
FINESS_COLS = [
    "rowtype",           # 0  : "structureet"
    "nofinesset",        # 1
    "nofinessej",        # 2
    "rs",                # 3  : raison sociale courte
    "rslongue",          # 4
    "complrs",           # 5
    "compllocalisation", # 6
    "numvoie",           # 7
    "typvoie",           # 8
    "libvoie",           # 9
    "compvoie",          # 10
    "lieuditbp",         # 11
    "commune",           # 12 : code 3 chiffres dans le département
    "departement",       # 13 : code 2 chiffres (ex : "01", "75", "2A")
    "libdepartement",    # 14
    "cpville",           # 15 : CP + nom commune (affichage)
    "telephone",         # 16
    "telecopie",         # 17
    "categetab",         # 18 : code catégorie (variable selon version)
    "libcategorie",      # 19 : libellé catégorie (utilisé pour le mapping)
    "categretab",        # 20
    "libcategretab",     # 21
    "siret",             # 22
    "codeape",           # 23
    "codemft",           # 24
    "libmft",            # 25
    "codesph",           # 26
    "libsph",            # 27
    "dateouverture",     # 28
    "dateautor",         # 29
    "datemaj",           # 30
    "numuai",            # 31 (présent ou absent selon les lignes)
]


async def get_finess_url() -> str:
    """Récupère l'URL du fichier FINESS depuis l'API data.gouv.fr."""
    print("  → Recherche URL FINESS sur data.gouv.fr...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        resp = await client.get(FINESS_DATASET_API)
        resp.raise_for_status()
    resources = resp.json().get("resources", [])
    # Chercher le CSV des établissements
    for r in resources:
        title = r.get("title", "").lower()
        fmt = r.get("format", "").lower()
        if fmt == "csv" and ("etablissement" in title or "stock" in title):
            print(f"  → Fichier trouvé : {r['title']}")
            return r["url"]
    # Fallback : plus grand CSV disponible
    csvs = [r for r in resources if r.get("format", "").lower() == "csv"]
    if csvs:
        r = max(csvs, key=lambda x: x.get("filesize", 0))
        print(f"  → Fallback sur : {r['title']}")
        return r["url"]
    raise ValueError(
        f"Fichier FINESS introuvable. Ressources disponibles : {[r.get('title') for r in resources[:5]]}"
    )


def libcat_to_label(libcat: str) -> str | None:
    """Mappe un libellé FINESS → label POI via correspondance textuelle."""
    lib = libcat.lower()
    for keyword, label in LIBCAT_MAP:
        if keyword in lib:
            return label
    return None


async def telecharger_finess(url: str) -> bytes:
    """Télécharge le fichier FINESS brut."""
    print(f"  → Téléchargement {url}")
    async with httpx.AsyncClient(follow_redirects=True, timeout=300) as client:
        resp = await client.get(url)
        resp.raise_for_status()
    print(f"  → {len(resp.content) / 1024 / 1024:.1f} Mo reçus")
    return resp.content


def agréger_par_commune(raw: bytes) -> dict[str, dict]:
    """
    Parse le fichier FINESS (format sans header) et agrège par commune INSEE.
    INSEE = departement (pos 13) + commune (pos 12, zero-padded 3 chiffres)
    """
    result: dict[str, dict] = {}
    nb_ok = 0
    nb_skip = 0
    try:
        lignes = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        lignes = raw.decode("latin-1").splitlines()

    # Ligne 0 = métadonnée finess;etalab;... → ignorer
    for ligne in lignes[1:]:
        champs = ligne.split(";")
        if len(champs) < 20:
            continue
        if champs[0] != "structureet":
            continue  # ignorer les lignes geolocalisation, etc.

        dept = champs[13].strip()
        commune_raw = champs[12].strip()
        libcat = champs[19].strip()

        # Construire le code INSEE 5 chiffres
        if not commune_raw or not dept:
            nb_skip += 1
            continue
        try:
            # Commune : 1-3 chiffres dans le département → zero-pad à 3
            commune_pad = commune_raw.zfill(3)
            # Corse : "2A" et "2B" ne sont pas numériques — cas spécial
            if dept in ("2A", "2B"):
                code_insee = dept + commune_pad
            else:
                code_insee = dept.zfill(2) + commune_pad
        except Exception:
            nb_skip += 1
            continue

        if len(code_insee) != 5:
            nb_skip += 1
            continue

        label = libcat_to_label(libcat)
        if not label:
            continue

        if code_insee not in result:
            result[code_insee] = {}
        result[code_insee][label] = result[code_insee].get(label, 0) + 1
        nb_ok += 1

    print(f"  → {nb_ok:,} établissements retenus, {nb_skip:,} ignorés (format)")
    print(f"  → {len(result):,} communes avec données FINESS")
    return result


async def run():
    print("=== Import FINESS (établissements de santé) ===\n")
    await init_db()

    # Migration colonne poi_detail
    async with async_session() as session:
        try:
            await session.execute(text("ALTER TABLE scores ADD COLUMN poi_detail TEXT"))
            await session.commit()
            print("  → Colonne poi_detail ajoutée\n")
        except Exception:
            pass  # déjà présente

    # Téléchargement
    print("Récupération URL FINESS...")
    url = await get_finess_url()
    print("Téléchargement FINESS...")
    raw = await telecharger_finess(url)

    # Agrégation
    print("\nAgrégation par commune...")
    poi_communes = agréger_par_commune(raw)

    # Merge en base — on fusionne avec le poi_detail existant
    print("\nMise à jour poi_detail en base...")
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
                print(f"  → {nb_maj:,} communes mises à jour...")

        await session.commit()

    print(f"  → {nb_maj:,} communes mises à jour avec données FINESS")
    print("\n=== Import FINESS terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
