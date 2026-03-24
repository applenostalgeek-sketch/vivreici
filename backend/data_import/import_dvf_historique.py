"""
Import DVF 2022 — prix médian historique pour calcul de tendance.
Même logique que import_dvf.py mais stocke dans prix_m2_median_2022.
Ne modifie PAS score_immobilier (qui reste basé sur DVF 2024).

À lancer une seule fois après import_dvf.py :
    python -m backend.data_import.import_dvf_historique
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.data_import.import_dvf import (
    DEPARTEMENTS, telecharger_departement,
    calculer_prix_median_par_commune,
)
import httpx
import asyncio as aio

ANNEE = "2022"
BASE_URL_HISTORIQUE = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{ANNEE}/departements/"
MAX_CONCURRENT = 10


async def telecharger_tous_departements_annee(annee: str):
    """Télécharge les fichiers DVF d'une année historique."""
    import httpx as _httpx
    from backend.data_import.import_dvf import COLONNES, DEPARTEMENTS
    import gzip, io
    import pandas as pd

    base = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/departements/"
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    print(f"Téléchargement DVF {annee} ({len(DEPARTEMENTS)} départements)...")

    async def dl(client, dep):
        url = f"{base}{dep}.csv.gz"
        async with sem:
            try:
                resp = await client.get(url, timeout=120)
                resp.raise_for_status()
                with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
                    df = pd.read_csv(gz, sep=",", dtype={"code_commune": str},
                                     usecols=lambda c: c in COLONNES, low_memory=False)
                return df
            except Exception as e:
                print(f"  [SKIP] Dept {dep}: {e}")
                return None

    async with _httpx.AsyncClient(follow_redirects=True) as client:
        tasks = [dl(client, dep) for dep in DEPARTEMENTS]
        resultats = await asyncio.gather(*tasks)

    import pandas as pd
    frames = [df for df in resultats if df is not None and not df.empty]
    print(f"  → {len(frames)} fichiers téléchargés")
    df_all = pd.concat(frames, ignore_index=True)
    print(f"  → {len(df_all):,} lignes totales")
    return df_all


async def run():
    await init_db()

    df_raw = await telecharger_tous_departements_annee(ANNEE)
    df_prix = calculer_prix_median_par_commune(df_raw)

    print(f"Mise à jour prix_m2_median_{ANNEE} en base...")
    async with async_session() as session:
        count = 0
        for _, row in df_prix.iterrows():
            await session.execute(text(f"""
                UPDATE scores
                SET prix_m2_median_{ANNEE} = :prix
                WHERE code_insee = :code
            """), {"prix": float(row["prix_m2_median"]), "code": row["code_insee"]})
            count += 1
            if count % 2000 == 0:
                await session.commit()
                print(f"  → {count} communes mises à jour")
        await session.commit()

    print(f"\nImport DVF {ANNEE} terminé — {count} communes avec prix historique.")

    # Vérification rapide
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT COUNT(*) FROM scores
            WHERE prix_m2_median > 0 AND prix_m2_median_2022 > 0
        """))
        nb = result.scalar()
        print(f"Communes avec les 2 années pour calcul tendance : {nb:,}")

        result2 = await session.execute(text("""
            SELECT
                AVG((prix_m2_median - prix_m2_median_2022) / prix_m2_median_2022 * 100)
            FROM scores
            WHERE prix_m2_median > 0 AND prix_m2_median_2022 > 0
        """))
        avg_change = result2.scalar()
        if avg_change:
            print(f"Variation prix moyenne 2022→2024 : {avg_change:+.1f}%")


if __name__ == "__main__":
    asyncio.run(run())
