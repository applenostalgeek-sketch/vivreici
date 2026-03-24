"""
Import des commerces depuis Sirene (INSEE).
Boulangeries, boucheries, supermarchés, épiceries par commune → poi_detail.

Source : Base Sirene des établissements — INSEE / data.gouv.fr
Fichier : StockEtablissement_utf8.zip (~500 Mo compressé)
URL : https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.zip

Durée estimée : ~10 min (téléchargement + lecture chunked 4 Go CSV)

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_sirene
"""

import asyncio
import json
import os
import sys
import tempfile
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
import pandas as pd

from sqlalchemy import text
from backend.database import async_session, init_db


SIRENE_URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.zip"

# Codes NAF (activité principale) → label POI
NAF_MAP = {
    # Boulangeries artisanales (production + vente)
    "1071C": "boulangerie",
    # Commerce de détail pain/pâtisserie
    "4724Z": "boulangerie",
    # Boucheries-charcuteries
    "4722Z": "boucherie",
    # Grandes surfaces alimentaires
    "4711D": "supermarché",   # Supermarchés (400–2499 m²)
    "4711E": "supermarché",   # Hypermarchés (≥ 2500 m²)
    "4711C": "supermarché",   # Hard discount (Aldi, Lidl, etc.)
    # Petites surfaces alimentaires
    "4711A": "épicerie",      # Supérettes (120–400 m²)
    "4711B": "épicerie",      # Magasins d'alimentation générale
}

# Colonnes nécessaires dans le CSV Sirene (minimise la mémoire)
COLS_NEEDED = [
    "codeCommuneEtablissement",
    "activitePrincipaleEtablissement",
    "etatAdministratifEtablissement",
]


async def telecharger_sirene(tmp_path: str) -> None:
    """Stream-download du ZIP Sirene vers un fichier temporaire."""
    print(f"  → Stream-download de {SIRENE_URL}")
    print(f"  → Destination temporaire : {tmp_path}")

    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        async with client.stream("GET", SIRENE_URL) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            reçu = 0
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):  # 1 Mo chunks
                    f.write(chunk)
                    reçu += len(chunk)
                    if total:
                        pct = reçu / total * 100
                        print(f"\r  → {reçu / 1024 / 1024:.0f} Mo / {total / 1024 / 1024:.0f} Mo ({pct:.0f}%)", end="", flush=True)
    print(f"\n  → Téléchargement terminé ({reçu / 1024 / 1024:.0f} Mo)")


def lire_et_agréger(zip_path: str) -> dict[str, dict]:
    """Lit le CSV Sirene en mode chunked et agrège les commerces par commune."""
    naf_voulus = set(NAF_MAP.keys())
    result: dict[str, dict] = {}

    with zipfile.ZipFile(zip_path) as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            raise ValueError("Aucun CSV dans le ZIP Sirene")
        csv_name = csv_files[0]
        print(f"  → Lecture de {csv_name} (lecture chunked par 200 000 lignes)...")

        with z.open(csv_name) as f:
            nb_chunks = 0
            nb_actifs = 0
            nb_retenus = 0

            for chunk in pd.read_csv(
                f,
                sep=",",
                encoding="utf-8",
                dtype=str,
                usecols=COLS_NEEDED,
                chunksize=200_000,
                low_memory=False,
            ):
                nb_chunks += 1

                # Filtrer : actifs uniquement
                actifs = chunk[chunk["etatAdministratifEtablissement"] == "A"]
                nb_actifs += len(actifs)

                # Filtrer : codes NAF voulus
                filtré = actifs[actifs["activitePrincipaleEtablissement"].isin(naf_voulus)]
                nb_retenus += len(filtré)

                for _, row in filtré.iterrows():
                    code = str(row["codeCommuneEtablissement"]).strip().zfill(5)
                    if len(code) != 5:
                        continue
                    label = NAF_MAP.get(row["activitePrincipaleEtablissement"].strip())
                    if not label:
                        continue
                    if code not in result:
                        result[code] = {}
                    result[code][label] = result[code].get(label, 0) + 1

                if nb_chunks % 50 == 0:
                    print(f"  → {nb_chunks * 200_000:,} lignes lues, {nb_retenus:,} commerces trouvés...")

    print(f"  → Total : {nb_actifs:,} établissements actifs analysés")
    print(f"  → {nb_retenus:,} commerces retenus")
    print(f"  → {len(result):,} communes avec données Sirene")
    return result


async def run():
    print("=== Import Sirene (commerces alimentaires) ===\n")
    await init_db()

    # Migration colonne poi_detail
    async with async_session() as session:
        try:
            await session.execute(text("ALTER TABLE scores ADD COLUMN poi_detail TEXT"))
            await session.commit()
            print("  → Colonne poi_detail ajoutée\n")
        except Exception:
            pass

    # Téléchargement dans un fichier temporaire
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()
    tmp_path = tmp.name

    try:
        print("Téléchargement Sirene (~500 Mo)...")
        await telecharger_sirene(tmp_path)

        # Lecture chunked + agrégation
        print("\nLecture et agrégation des commerces...")
        poi_communes = lire_et_agréger(tmp_path)

    finally:
        # Nettoyage fichier temporaire
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
            print(f"  → Fichier temporaire supprimé")

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

    print(f"  → {nb_maj:,} communes mises à jour avec données Sirene")
    print("\n=== Import Sirene terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
