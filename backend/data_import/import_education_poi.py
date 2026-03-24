"""
Import des établissements scolaires depuis l'Annuaire de l'éducation.
Maternelles, primaires, collèges, lycées par commune → poi_detail.

Source : Ministère de l'Éducation nationale — data.education.gouv.fr
Dataset : https://data.education.gouv.fr/explore/dataset/fr-en-annuaire-education/

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_education_poi
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


EDU_BASE_URL = (
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets"
    "/fr-en-annuaire-education/exports/csv"
)
EDU_PAGE_SIZE = 10000

# Type d'établissement → label POI (correspondances Annuaire éducation)
TYPE_MAP = {
    "École maternelle":           "école_maternelle",
    "École maternelle publique":  "école_maternelle",
    "École maternelle privée":    "école_maternelle",
    "École primaire":             "école_primaire",
    "École élémentaire":          "école_primaire",
    "École élémentaire publique": "école_primaire",
    "École élémentaire privée":   "école_primaire",
    "Collège":                    "collège",
    "Lycée":                      "lycée",
    "Lycée polyvalent":           "lycée",
    "Lycée technologique":        "lycée",
    "EREA":                       "lycée",
    "Lycée professionnel":        "lycée_professionnel",
    "Lycée professionnel privé":  "lycée_professionnel",
}


async def telecharger_annuaire() -> pd.DataFrame:
    """Télécharge l'Annuaire de l'éducation en un seul appel (limit=100000)."""
    url = f"{EDU_BASE_URL}?limit=100000&lang=fr&timezone=Europe%2FParis"
    print(f"  → Téléchargement Annuaire éducation (tous établissements, timeout 10 min)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    print(f"  → {len(resp.content) / 1024 / 1024:.1f} Mo reçus")
    df = pd.read_csv(io.BytesIO(resp.content), sep=";", dtype=str, low_memory=False)
    df.columns = [c.strip().lstrip('\ufeff') for c in df.columns]  # enlever BOM éventuel
    print(f"  → {len(df):,} établissements. Colonnes : {list(df.columns[:8])}")
    return df


def agréger_par_commune(df: pd.DataFrame) -> dict[str, dict]:
    """Filtre les établissements ouverts et agrège par commune."""
    # Identifier la colonne code INSEE commune
    commune_col = None
    for col in ["Code_commune_INSEE", "code_commune_insee", "Code commune INSEE",
                "code_commune", "codecommune", "code_postal"]:
        if col in df.columns:
            commune_col = col
            break

    if commune_col is None:
        print(f"  ERREUR : colonne commune introuvable. Colonnes : {list(df.columns[:20])}")
        raise ValueError("Impossible de déterminer la colonne commune dans l'Annuaire éducation")

    print(f"  → Colonne commune : '{commune_col}'")

    # Identifier la colonne type d'établissement
    type_col = None
    for col in ["Type_etablissement", "type_etablissement", "Type d'établissement",
                "nature_uai_libe", "type"]:
        if col in df.columns:
            type_col = col
            break

    if type_col is None:
        print(f"  ERREUR : colonne type introuvable. Colonnes : {list(df.columns[:20])}")
        raise ValueError("Impossible de déterminer la colonne type dans l'Annuaire éducation")

    print(f"  → Colonne type : '{type_col}'")

    # Filtrer les établissements ouverts
    etat_col = None
    for col in ["etat", "Etat_etablissement", "etat_etablissement", "Etat etablissement", "statut_etablissement"]:
        if col in df.columns:
            etat_col = col
            break

    if etat_col:
        avant = len(df)
        df = df[df[etat_col].astype(str).str.strip().isin(["1", "OUVERT"])].copy()
        print(f"  → {len(df):,} établissements ouverts (sur {avant:,})")
    else:
        print(f"  → Colonne état introuvable, pas de filtrage sur le statut")

    # Colonnes booléennes pour les écoles et lycées pro
    has_mat_col = "ecole_maternelle" in df.columns
    has_ele_col = "ecole_elementaire" in df.columns
    has_pro_col = "voie_professionnelle" in df.columns

    # Agrégation
    result: dict[str, dict] = {}
    types_inconnus = set()

    for _, row in df.iterrows():
        code = str(row[commune_col]).strip().zfill(5)  # padding pour codes Corse (2A/2B → string)
        if not code or code == "nan":
            continue
        # Normaliser : certains codes peuvent être "75001" (arrondissements OK)
        if len(code) < 4 or len(code) > 5:
            continue

        typ = str(row[type_col]).strip()
        label = TYPE_MAP.get(typ)

        # "Ecole" = type générique → déduire via colonnes booléennes
        if label is None and typ == "Ecole":
            is_mat = has_mat_col and str(row.get("ecole_maternelle", "")).strip() == "1"
            is_ele = has_ele_col and str(row.get("ecole_elementaire", "")).strip() == "1"
            if is_mat:
                label = "école_maternelle"
            elif is_ele:
                label = "école_primaire"

        # "Lycée" = peut être général/techno/pro → vérifier voie_professionnelle
        if label == "lycée" and has_pro_col:
            if str(row.get("voie_professionnelle", "")).strip() == "1":
                label = "lycée_professionnel"

        if label is None:
            types_inconnus.add(typ)
            continue

        if code not in result:
            result[code] = {}
        result[code][label] = result[code].get(label, 0) + 1

    if types_inconnus:
        print(f"  → Types non mappés (ignorés) : {sorted(types_inconnus)[:10]}")

    print(f"  → {len(result):,} communes avec données éducation")
    return result


async def run():
    print("=== Import Annuaire Éducation (établissements scolaires) ===\n")
    await init_db()

    # Migration colonne poi_detail
    async with async_session() as session:
        try:
            await session.execute(text("ALTER TABLE scores ADD COLUMN poi_detail TEXT"))
            await session.commit()
            print("  → Colonne poi_detail ajoutée\n")
        except Exception:
            pass

    # Téléchargement
    df = await telecharger_annuaire()

    # Agrégation
    print("\nAgrégation par commune...")
    poi_communes = agréger_par_commune(df)

    # Merge en base
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

    print(f"  → {nb_maj:,} communes mises à jour avec données éducation")
    print("\n=== Import Annuaire Éducation terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
