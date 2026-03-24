"""
Import des équipements sportifs depuis le RES (Recensement des Équipements Sportifs).
Piscines, gymnases, terrains, stades par commune → poi_detail.

Source : Ministère des Sports — data.sports.gouv.fr
Dataset : https://data.sports.gouv.fr/explore/dataset/equipements-sportifs-espaces-et-sites-de-pratiques/

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_res
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


# URL correcte — equipements.sports.gouv.fr, dataset data-es
RES_URL_V2 = (
    "https://equipements.sports.gouv.fr/api/explore/v2.1/catalog/datasets"
    "/data-es/exports/csv?limit=500000&lang=fr&timezone=Europe%2FParis"
)
# Champs clés du dataset data-es :
#   new_code         : code INSEE commune (5 chiffres)
#   equip_type_famille : famille d'équipement (ex: "Piscines", "Gymnases - Halls")

# Famille d'équipement → label POI
FAMILLE_MAP = {
    "Piscines":                       "piscine",
    "Piscine":                        "piscine",
    "Bassin de natation":             "piscine",
    "Gymnases - Halls":               "gymnase",
    "Gymnases":                       "gymnase",
    "Salles et terrains couverts":    "gymnase",
    "Salle multisports":              "gymnase",
    "Salle non spécialisée":          "gymnase",
    "Football":                       "terrain_football",
    "Sports de ballon":               "terrain_football",
    "Stades":                         "stade",
    "Stades d'athlétisme":            "stade",
    "Terrains de grands jeux":        "terrain_football",
    "Terrains":                       "terrain_football",
}

# Subset de types d'équipements qu'on inclut (si colonne type disponible)
# Garde uniquement les équipements "structurants" pour une commune
TYPES_INCLUS_KEYWORDS = [
    "piscine", "gymnase", "hall", "stade", "foot", "terrain",
    "salle multisports", "salle omnisports", "palais des sports",
]


async def telecharger_res() -> pd.DataFrame | None:
    """Télécharge le fichier RES depuis equipements.sports.gouv.fr."""
    print(f"  → Téléchargement RES (equipements.sports.gouv.fr, ~350 000 équipements)...")
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
            resp = await client.get(RES_URL_V2)
            resp.raise_for_status()
        print(f"  → {len(resp.content) / 1024 / 1024:.1f} Mo reçus")
        df = pd.read_csv(io.BytesIO(resp.content), sep=";", dtype=str, low_memory=False)
        df.columns = [c.strip().lstrip('\ufeff') for c in df.columns]
        return df
    except Exception as e:
        print(f"  → ERREUR : {e}")
        return None


def agréger_par_commune(df: pd.DataFrame) -> dict[str, dict]:
    """Agrège les équipements sportifs par commune."""
    df.columns = [c.strip() for c in df.columns]
    print(f"  → {len(df):,} équipements. Colonnes : {list(df.columns[:12])}")

    # Identifier colonne code commune (data-es utilise "new_code")
    commune_col = None
    for col in ["new_code", "CommuneCode", "codecommune", "Numero_Commune", "code_commune",
                "CodeCommune", "commune_code", "dep_com"]:
        if col in df.columns:
            commune_col = col
            break

    if commune_col is None:
        # Chercher une colonne dont les valeurs ressemblent à des codes INSEE
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(100)
            if sample.str.match(r"^\d{5}$").mean() > 0.5:
                commune_col = col
                print(f"  → Colonne commune détectée automatiquement : '{col}'")
                break

    if commune_col is None:
        print(f"  ERREUR : colonne commune introuvable dans RES")
        raise ValueError(f"Colonnes disponibles : {list(df.columns)}")

    print(f"  → Colonne commune : '{commune_col}'")

    # Identifier colonne famille d'équipement (data-es utilise "equip_type_famille")
    famille_col = None
    for col in ["equip_type_famille", "EquipementFamilleLib", "equipementfamillelib",
                "Famille_equipement", "famille_equipement", "FamilleEquipement",
                "InstallationType", "TypeInstallation", "type_equipement", "EquipementTypeLib"]:
        if col in df.columns:
            famille_col = col
            break

    if famille_col is None:
        print(f"  ERREUR : colonne famille équipement introuvable")
        print(f"  Colonnes : {list(df.columns)}")
        raise ValueError("Colonne famille équipement introuvable dans RES")

    print(f"  → Colonne famille : '{famille_col}'")

    # Compter les types uniques pour info
    types_uniques = df[famille_col].dropna().unique()
    print(f"  → {len(types_uniques)} types d'équipements distincts")

    result: dict[str, dict] = {}
    familles_non_mappées = set()

    for _, row in df.iterrows():
        code = str(row[commune_col]).strip().zfill(5)
        if len(code) != 5:
            continue

        famille = str(row[famille_col]).strip() if pd.notna(row[famille_col]) else ""
        label = FAMILLE_MAP.get(famille)

        if label is None:
            # Tentative de correspondance partielle sur mot-clé
            famille_lower = famille.lower()
            for kw, lbl in [("piscine", "piscine"), ("gymnase", "gymnase"), ("hall", "gymnase"),
                             ("stade", "stade"), ("football", "terrain_football"),
                             ("foot", "terrain_football"), ("terrain", "terrain_football")]:
                if kw in famille_lower:
                    label = lbl
                    break
            if label is None:
                familles_non_mappées.add(famille)
                continue

        if code not in result:
            result[code] = {}
        result[code][label] = result[code].get(label, 0) + 1

    if familles_non_mappées:
        print(f"  → Familles non mappées (ignorées, {len(familles_non_mappées)} types) : "
              f"{sorted(familles_non_mappées)[:8]}")

    print(f"  → {len(result):,} communes avec données sportives")
    return result


async def run():
    print("=== Import RES (équipements sportifs) ===\n")
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
    print("Téléchargement RES...")
    df = await telecharger_res()
    if df is None:
        print("ERREUR : impossible de télécharger les données RES.")
        return

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

    print(f"  → {nb_maj:,} communes mises à jour avec données RES")
    print("\n=== Import RES terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
