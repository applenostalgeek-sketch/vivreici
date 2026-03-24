"""
Import données revenus et pauvreté — INSEE Filosofi 2021 (dernier millésime disponible)
Source : https://www.insee.fr/fr/statistiques/7756729

Fichier : cc_filosofi_2021_COM.csv
Colonnes utilisées :
  - CODGEO : code INSEE commune
  - MED21  : niveau de vie médian (€/an) — stocké pour affichage uniquement
  - TP6021 : taux de pauvreté au seuil de 60% (%)

Les valeurs 's' (secret statistique, communes < ~1000 ménages fiscaux) sont ignorées.

Score cohésion sociale (stocké dans score_revenus) :
  - Basé uniquement sur TP6021 percentile inverse (moins de pauvreté = mieux)
  - MED21 n'entre plus dans le score pour éviter de récompenser la ségrégation par revenus
  - Revenu médian conservé en données brutes pour information
"""

import asyncio
import httpx
import zipfile
import io
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import percentile_to_score, calculer_score_global

FILOSOFI_URL = "https://www.insee.fr/fr/statistiques/fichier/7756729/base-cc-filosofi-2021-geo2024_csv.zip"
CSV_COMMUNE = "cc_filosofi_2021_COM.csv"


async def telecharger_filosofi() -> pd.DataFrame:
    """Télécharge et parse le fichier Filosofi communes."""
    print("Téléchargement Filosofi 2020 (revenus par commune)...")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(FILOSOFI_URL, timeout=120)
        resp.raise_for_status()

    print(f"  → Reçu ({len(resp.content)/1024/1024:.1f} Mo). Extraction CSV...")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        # Chercher le fichier communes (peut être dans un sous-dossier)
        csv_files = [f for f in z.namelist() if "COM" in f.upper() and f.endswith(".csv")]
        if not csv_files:
            raise ValueError(f"Fichier COM non trouvé. Contenu zip: {z.namelist()}")

        target = csv_files[0]
        print(f"  → Lecture de {target}")
        with z.open(target) as f:
            df = pd.read_csv(f, sep=";", dtype={"CODGEO": str}, low_memory=False)

    print(f"  → {len(df):,} communes chargées")
    return df


def calculer_scores_revenus(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le score revenus par commune.
    MED20 et TP6020 peuvent avoir des valeurs 's' (secret statistique).
    """
    df = df[["CODGEO", "MED21", "TP6021"]].copy()
    df = df.rename(columns={"CODGEO": "code_insee", "MED21": "revenu_median", "TP6021": "taux_pauvrete"})

    # Nettoyage : remplacer 's' par NaN
    df["revenu_median"] = pd.to_numeric(df["revenu_median"], errors="coerce")
    df["taux_pauvrete"] = pd.to_numeric(
        df["taux_pauvrete"].astype(str).str.replace(",", "."), errors="coerce"
    )

    # Code INSEE → 5 chiffres
    df["code_insee"] = df["code_insee"].astype(str).str.strip().str.zfill(5)

    # Communes avec au moins une des deux métriques
    df_valides = df[df["revenu_median"].notna() | df["taux_pauvrete"].notna()].copy()
    print(f"  → {len(df_valides):,} communes avec données Filosofi")

    # Score revenus médian (percentile direct)
    serie_med = df_valides["revenu_median"].dropna()
    df_valides["score_mediane"] = df_valides["revenu_median"].apply(
        lambda x: percentile_to_score(x, serie_med, "direct") if pd.notna(x) else -1.0
    )

    # Score taux pauvreté (percentile inverse)
    serie_pauv = df_valides["taux_pauvrete"].dropna()
    df_valides["score_pauvrete"] = df_valides["taux_pauvrete"].apply(
        lambda x: percentile_to_score(x, serie_pauv, "inverse") if pd.notna(x) else -1.0
    )

    # Score cohésion sociale = uniquement taux de pauvreté (inversé)
    # Le revenu médian n'entre plus dans le score : récompenser les hauts revenus
    # crée un biais ségrégant (zones CSP+ = A, zones populaires = D indépendamment des services)
    df_valides["score_revenus"] = df_valides["score_pauvrete"]
    df_scored = df_valides[df_valides["score_revenus"] >= 0]

    print(f"  → {len(df_scored):,} communes avec score revenus calculé")
    print(f"  → Revenu médian min/médiane/max : "
          f"{serie_med.min():.0f} / {serie_med.median():.0f} / {serie_med.max():.0f} €")
    print(f"  → Taux pauvreté min/médiane/max : "
          f"{serie_pauv.min():.1f} / {serie_pauv.median():.1f} / {serie_pauv.max():.1f} %")

    return df_scored[["code_insee", "score_revenus", "revenu_median", "taux_pauvrete"]]


async def run():
    print("=== Import données revenus (Filosofi 2021) ===\n")
    await init_db()

    df_raw = await telecharger_filosofi()

    print("Calcul des scores revenus...")
    df = calculer_scores_revenus(df_raw)

    print("\nUpsert en base (table scores)...")
    async with async_session() as session:
        nb_updated, nb_inserted = 0, 0

        for _, row in df.iterrows():
            code = row["code_insee"]
            score = float(row["score_revenus"])
            revenu = float(row["revenu_median"]) if pd.notna(row["revenu_median"]) else 0.0
            pauvrete = float(row["taux_pauvrete"]) if pd.notna(row["taux_pauvrete"]) else 0.0

            result = await session.execute(
                text("""
                    UPDATE scores
                    SET score_revenus = :s,
                        revenu_median = :revenu,
                        taux_pauvrete = :pauvrete,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE code_insee = :c
                """),
                {"s": score, "revenu": revenu, "pauvrete": pauvrete, "c": code}
            )
            if result.rowcount == 0:
                await session.execute(text("""
                    INSERT OR IGNORE INTO scores
                        (code_insee, score_global, lettre,
                         score_equipements, score_securite, score_immobilier,
                         score_education, score_sante, score_environnement,
                         score_demographie, score_revenus,
                         nb_categories_scorees,
                         revenu_median, taux_pauvrete,
                         updated_at)
                    VALUES
                        (:c, 50, 'C', -1, -1, -1, -1, -1, -1, -1, :s, 1, :revenu, :pauvrete, CURRENT_TIMESTAMP)
                """), {"c": code, "s": score, "revenu": revenu, "pauvrete": pauvrete})
                nb_inserted += 1
            else:
                nb_updated += 1

            if (nb_updated + nb_inserted) % 5000 == 0:
                await session.commit()
                print(f"  {nb_updated + nb_inserted:,} communes traitées...")

        await session.commit()
        print(f"  → {nb_updated:,} mises à jour, {nb_inserted:,} créées")

        # Recalcul scores globaux
        print("\nRecalcul des scores globaux (avec catégorie revenus)...")
        result = await session.execute(text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_education, score_sante, score_revenus,
                   score_environnement, score_demographie
            FROM scores
            WHERE score_equipements >= 0 OR score_securite >= 0
               OR score_immobilier >= 0 OR score_education >= 0
               OR score_sante >= 0 OR score_revenus >= 0
               OR score_environnement >= 0 OR score_demographie >= 0
        """))
        rows = result.fetchall()
        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_education", "score_sante", "score_revenus",
                "score_environnement", "score_demographie"]
        cat_map = {
            "score_equipements": "equipements", "score_securite": "securite",
            "score_immobilier": "immobilier", "score_education": "education",
            "score_sante": "sante", "score_revenus": "revenus",
            "score_environnement": "environnement", "score_demographie": "demographie",
        }
        nb_recalc = 0
        for row in rows:
            r = dict(zip(cols, row))
            sous_scores = {cat: r[col] for col, cat in cat_map.items()
                           if r[col] is not None and r[col] >= 0}
            if not sous_scores:
                continue
            score, lettre, nb = calculer_score_global(sous_scores)
            await session.execute(text("""
                UPDATE scores SET score_global=:sg, lettre=:l,
                    nb_categories_scorees=:nb, updated_at=CURRENT_TIMESTAMP
                WHERE code_insee=:c
            """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
            nb_recalc += 1
            if nb_recalc % 5000 == 0:
                await session.commit()
        await session.commit()
        print(f"  → {nb_recalc:,} scores globaux recalculés")

    print("\n=== Résumé ===")
    async with async_session() as session:
        result = await session.execute(text(
            "SELECT COUNT(*) FROM scores WHERE score_revenus >= 0"
        ))
        print(f"Communes avec score revenus : {result.scalar():,}")

        result = await session.execute(text("""
            SELECT s.code_insee, c.nom, c.departement,
                   s.score_revenus, s.revenu_median, s.taux_pauvrete, s.score_global
            FROM scores s
            LEFT JOIN communes c ON c.code_insee = s.code_insee
            WHERE s.score_revenus >= 0
            ORDER BY s.score_revenus DESC LIMIT 10
        """))
        print("\nTop 10 communes par score revenus :")
        for r in result.fetchall():
            nom = r[1] or "?"
            print(f"  {r[0]}  {nom:<35} {r[2]}  rev={r[4]:.0f}€  pauv={r[5]:.1f}%  score={r[3]:.1f}  global={r[6]:.1f}")


if __name__ == "__main__":
    asyncio.run(run())
