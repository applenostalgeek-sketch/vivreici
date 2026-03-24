"""
Import données criminalité SSMSI — Ministère de l'Intérieur
Source : data.gouv.fr — bases statistiques communales de la délinquance
         enregistrée par la police et la gendarmerie nationales

Méthode :
- Fichier CSV.gz communal, millésime 2024 (données jusqu'en 2024, géographie 2025)
- 4 catégories principales : cambriolages, violences physiques (intra+hors famille),
  vols sans violence, vols violents (avec et sans armes)
- Taux pour mille habitants, année 2024 uniquement
- Score percentile inversé : moins de crimes = meilleur score
"""

import asyncio
import httpx
import gzip
import pandas as pd
import io
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import percentile_to_score, calculer_score_global, CATEGORIES


# URL du fichier CSV communal SSMSI (millésime 2024, publié juin 2025)
SSMSI_URL = (
    "https://static.data.gouv.fr/resources/"
    "bases-statistiques-communale-departementale-et-regionale-de-la-delinquance"
    "-enregistree-par-la-police-et-la-gendarmerie-nationales/"
    "20250710-144817/"
    "donnee-data.gouv-2024-geographie2025-produit-le2025-06-04.csv.gz"
)

# Année de référence
ANNEE = 2024

# Catégories de délits retenues pour le taux global de criminalité
# (libellés exacts du fichier SSMSI)
CATEGORIES_DELITS = [
    "Cambriolages de logement",
    "Violences physiques hors cadre familial",
    "Violences physiques intrafamiliales",
    "Vols sans violence contre des personnes",
    "Vols violents sans arme",
    "Vols avec armes",
]


async def telecharger_csv() -> pd.DataFrame:
    """Télécharge et décompresse le fichier CSV SSMSI."""
    print(f"Téléchargement du fichier SSMSI ({SSMSI_URL[-60:]})...")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(SSMSI_URL, timeout=300)
        resp.raise_for_status()

    taille_mo = len(resp.content) / 1024 / 1024
    print(f"  → Reçu ({taille_mo:.1f} Mo). Décompression et parsing...")

    content = gzip.decompress(resp.content)
    df = pd.read_csv(
        io.BytesIO(content),
        sep=";",
        quotechar='"',
        dtype={"CODGEO_2025": str},
        low_memory=False,
    )
    print(f"  → {len(df):,} lignes chargées, {df['CODGEO_2025'].nunique():,} communes au total")
    return df


def calculer_taux_criminalite(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule un taux de criminalité global par commune.

    Méthode :
    - Filtre sur l'année 2024 et les 6 catégories retenues
    - Convertit taux_pour_mille (format FR avec virgule) en float
    - Pour les valeurs 'ndiff' (non diffusées) : taux NaN → traité comme 0
      (petites communes, effectifs trop faibles pour être publiés)
    - Taux global = somme des taux pour mille des catégories disponibles
    """
    # Filtrer sur l'année et les catégories voulues
    masque = (df["annee"] == ANNEE) & (df["indicateur"].isin(CATEGORIES_DELITS))
    df_filtre = df[masque].copy()
    print(f"  → {len(df_filtre):,} lignes filtrées ({ANNEE}, {len(CATEGORIES_DELITS)} catégories)")

    # Convertir le taux (séparateur décimal = virgule)
    df_filtre["taux"] = (
        df_filtre["taux_pour_mille"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .replace({"nan": None, "NA": None, "": None})
    )
    df_filtre["taux"] = pd.to_numeric(df_filtre["taux"], errors="coerce")

    # Pour les ndiff (non diffusé pour secret statistique) : on traite NaN comme 0
    # Logique : communes trop petites → faits trop rares → vraiment peu de criminalité
    df_filtre["taux"] = df_filtre["taux"].fillna(0.0)

    # Agréger : somme des taux par commune
    df_agg = (
        df_filtre.groupby("CODGEO_2025")["taux"]
        .sum()
        .reset_index()
        .rename(columns={"CODGEO_2025": "code_insee", "taux": "taux_criminalite"})
    )

    print(f"  → {len(df_agg):,} communes avec données criminalité")
    print(f"  → Taux min/median/max : "
          f"{df_agg['taux_criminalite'].min():.1f} / "
          f"{df_agg['taux_criminalite'].median():.1f} / "
          f"{df_agg['taux_criminalite'].max():.1f} pour mille")
    return df_agg


async def run():
    """Import complet : téléchargement → scoring → upsert en base."""
    await init_db()

    # 1. Téléchargement
    df_raw = await telecharger_csv()

    # 2. Calcul du taux de criminalité par commune
    print("Calcul du taux de criminalité...")
    df_securite = calculer_taux_criminalite(df_raw)
    del df_raw  # libérer la mémoire (fichier ~34 Mo)

    # 3. Calcul des scores par percentile (sens inverse : moins = mieux)
    print("Calcul des scores percentile...")
    serie_taux = df_securite["taux_criminalite"]
    df_securite["score_securite"] = df_securite["taux_criminalite"].apply(
        lambda x: percentile_to_score(x, serie_taux, "inverse")
    )
    print(f"  → Score min/median/max : "
          f"{df_securite['score_securite'].min():.1f} / "
          f"{df_securite['score_securite'].median():.1f} / "
          f"{df_securite['score_securite'].max():.1f}")

    # 4. Upsert dans la table scores (UPDATE uniquement, pas d'INSERT complet)
    print("Mise à jour de la base de données...")
    async with async_session() as session:

        # Récupérer TOUS les scores existants en base (pour recalculer le global)
        result = await session.execute(
            text("""
                SELECT code_insee,
                       score_equipements, score_securite, score_immobilier,
                       score_demographie, score_education, score_sante, score_environnement
                FROM scores
            """)
        )
        rows = result.fetchall()
        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_demographie", "score_education", "score_sante", "score_environnement"]
        df_scores_base = pd.DataFrame(rows, columns=cols)
        print(f"  → {len(df_scores_base):,} entrées existantes en base")

        # Merger les nouveaux scores sécurité
        df_scores_base = df_scores_base.merge(
            df_securite[["code_insee", "taux_criminalite", "score_securite"]],
            on="code_insee",
            how="left",
            suffixes=("_old", "_new"),
        )
        # Utiliser le nouveau score sécurité là où disponible
        mask_new = df_scores_base["score_securite_new"].notna()
        df_scores_base.loc[mask_new, "score_securite"] = df_scores_base.loc[mask_new, "score_securite_new"]
        df_scores_base.loc[mask_new, "taux_criminalite"] = df_scores_base.loc[mask_new, "taux_criminalite"]
        df_scores_base["taux_criminalite"] = df_scores_base.get("taux_criminalite", 0.0).fillna(0.0)

        nb_updates = mask_new.sum()
        print(f"  → {nb_updates:,} communes avec données sécurité à mettre à jour")

        # UPDATE par batch
        count = 0
        for _, row in df_scores_base.iterrows():
            code = row["code_insee"]
            new_score_sec = row.get("score_securite_new")
            taux = row.get("taux_criminalite", 0.0)

            if pd.isna(new_score_sec):
                # Pas de donnée sécurité pour cette commune → ne pas toucher score_securite
                continue

            # Recalculer score_global avec les scores actuels (y compris le nouveau securite)
            sous_scores = {
                "equipements": float(row["score_equipements"]) if not pd.isna(row["score_equipements"]) else -1.0,
                "securite":    float(new_score_sec),
                "immobilier":  float(row["score_immobilier"]) if not pd.isna(row["score_immobilier"]) else -1.0,
                "demographie": float(row["score_demographie"]) if not pd.isna(row["score_demographie"]) else -1.0,
                "education":   float(row["score_education"]) if not pd.isna(row["score_education"]) else -1.0,
                "sante":       float(row["score_sante"]) if not pd.isna(row["score_sante"]) else -1.0,
                "environnement": float(row["score_environnement"]) if not pd.isna(row["score_environnement"]) else -1.0,
            }
            score_global, lettre, nb_cat = calculer_score_global(sous_scores)

            await session.execute(
                text("""
                    UPDATE scores
                    SET score_securite    = :score_sec,
                        taux_criminalite  = :taux,
                        score_global      = :score_global,
                        lettre            = :lettre,
                        nb_categories_scorees = :nb_cat,
                        updated_at        = :now
                    WHERE code_insee = :code
                """),
                {
                    "score_sec":    round(float(new_score_sec), 1),
                    "taux":         round(float(taux), 4),
                    "score_global": score_global,
                    "lettre":       lettre,
                    "nb_cat":       nb_cat,
                    "now":          datetime.utcnow(),
                    "code":         code,
                },
            )
            count += 1
            if count % 5000 == 0:
                await session.commit()
                print(f"  → {count}/{nb_updates} communes mises à jour")

        await session.commit()

    print(f"\nTerminé. {count} communes mises à jour avec le score sécurité.")


if __name__ == "__main__":
    asyncio.run(run())
