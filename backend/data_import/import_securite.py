"""
Import données criminalité SSMSI — Ministère de l'Intérieur
Source : data.gouv.fr — bases statistiques communales de la délinquance
         enregistrée par la police et la gendarmerie nationales

Méthode :
- Fichier CSV.gz communal, millésime 2025 (données jusqu'en 2025, géographie 2025)
- 4 catégories principales : cambriolages, violences physiques (intra+hors famille),
  vols sans violence, vols violents (avec et sans armes)
- Taux pour mille habitants, année 2025 uniquement
- Score percentile inversé : moins de crimes = meilleur score

Gestion du secret statistique SSMSI :
- Communes avec ≤5 faits sur 3 ans consécutifs → 'ndiff' (non diffusé)
- Le fichier fournit `complement_info_taux` : moyenne départementale des communes ndiff
- On utilise cette valeur comme estimation (meilleure approximation disponible)
- Avant ce fix (avr. 2026) : ndiff traité comme 0 → 82% des communes scoraient 100
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


# URL du fichier CSV communal SSMSI (millésime 2025, publié mars 2026)
SSMSI_URL = (
    "https://static.data.gouv.fr/resources/"
    "bases-statistiques-communale-departementale-et-regionale-de-la-delinquance"
    "-enregistree-par-la-police-et-la-gendarmerie-nationales/"
    "20260326-124144/"
    "donnee-data.gouv-2025-geographie2025-produit-le2026-02-03.csv.gz"
)

# Année de référence
ANNEE = 2025

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
    - Filtre sur l'année 2025 et les 6 catégories retenues
    - Convertit taux_pour_mille (format FR avec virgule) en float
    - Pour les communes 'ndiff' (secret statistique, ≤5 faits sur 3 ans) :
      utilise complement_info_taux (moyenne départementale des communes ndiff)
    - Taux global = somme des taux pour mille des catégories disponibles
    """
    # Filtrer sur l'année et les catégories voulues
    masque = (df["annee"] == ANNEE) & (df["indicateur"].isin(CATEGORIES_DELITS))
    df_filtre = df[masque].copy()
    print(f"  → {len(df_filtre):,} lignes filtrées ({ANNEE}, {len(CATEGORIES_DELITS)} catégories)")

    # Stats sur la diffusion
    nb_diff = (df_filtre["est_diffuse"] == "diff").sum()
    nb_ndiff = (df_filtre["est_diffuse"] == "ndiff").sum()
    print(f"  → {nb_diff:,} lignes diffusées, {nb_ndiff:,} lignes ndiff (secret statistique)")

    # Convertir le taux principal (séparateur décimal = virgule)
    df_filtre["taux"] = (
        df_filtre["taux_pour_mille"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .replace({"nan": None, "NA": None, "": None})
    )
    df_filtre["taux"] = pd.to_numeric(df_filtre["taux"], errors="coerce")

    # Convertir le complement_info_taux (fallback départemental pour ndiff)
    df_filtre["taux_fallback"] = (
        df_filtre["complement_info_taux"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .replace({"nan": None, "NA": None, "": None})
    )
    df_filtre["taux_fallback"] = pd.to_numeric(df_filtre["taux_fallback"], errors="coerce")

    # Pour les ndiff : utiliser complement_info_taux comme estimation
    # (moyenne départementale des communes ndiff = meilleure approximation SSMSI)
    mask_ndiff = (df_filtre["est_diffuse"] == "ndiff") & df_filtre["taux"].isna()
    nb_fallback_used = mask_ndiff.sum()
    nb_fallback_available = (mask_ndiff & df_filtre["taux_fallback"].notna()).sum()
    df_filtre.loc[mask_ndiff & df_filtre["taux_fallback"].notna(), "taux"] = (
        df_filtre.loc[mask_ndiff & df_filtre["taux_fallback"].notna(), "taux_fallback"]
    )
    print(f"  → {nb_fallback_used:,} lignes ndiff sans taux, {nb_fallback_available:,} corrigées via complement_info_taux")

    # Les lignes restantes sans taux (ndiff sans fallback) → NaN, ignorées dans la somme
    nb_still_missing = df_filtre["taux"].isna().sum()
    if nb_still_missing > 0:
        print(f"  ⚠ {nb_still_missing:,} lignes restent sans taux (ndiff sans complement_info_taux)")

    # Agréger : somme des taux par commune (NaN ignorés par défaut)
    df_agg = (
        df_filtre.groupby("CODGEO_2025")["taux"]
        .sum(min_count=1)  # min_count=1 : NaN si aucune valeur non-NaN
        .reset_index()
        .rename(columns={"CODGEO_2025": "code_insee", "taux": "taux_criminalite"})
    )

    # Communes entièrement sans données → exclure
    nb_avant = len(df_agg)
    df_agg = df_agg.dropna(subset=["taux_criminalite"])
    nb_exclu = nb_avant - len(df_agg)
    if nb_exclu > 0:
        print(f"  → {nb_exclu:,} communes sans aucune donnée de taux exclues")

    print(f"  → {len(df_agg):,} communes avec données criminalité")
    print(f"  → Taux min/median/max : "
          f"{df_agg['taux_criminalite'].min():.2f} / "
          f"{df_agg['taux_criminalite'].median():.2f} / "
          f"{df_agg['taux_criminalite'].max():.2f} pour mille")

    # Distribution par quartile pour vérifier que ce n'est plus binaire
    q = df_agg["taux_criminalite"].quantile([0.1, 0.25, 0.5, 0.75, 0.9])
    print(f"  → Quantiles : P10={q[0.1]:.2f}  P25={q[0.25]:.2f}  P50={q[0.5]:.2f}  P75={q[0.75]:.2f}  P90={q[0.9]:.2f}")
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

    # Vérifier que la distribution n'est plus binaire
    pct_100 = (df_securite["score_securite"] >= 99.5).mean() * 100
    print(f"  → {pct_100:.1f}% des communes avec score >= 99.5 (avant fix: ~82%)")

    # 4. Upsert dans la table scores (UPDATE uniquement, pas d'INSERT complet)
    print("Mise à jour de la base de données...")
    async with async_session() as session:

        # Récupérer TOUS les scores existants (9 catégories pour recalcul global)
        result = await session.execute(
            text("""
                SELECT code_insee,
                       score_equipements, score_securite, score_immobilier,
                       score_demographie, score_education, score_sante,
                       score_environnement, score_transports, score_risques
                FROM scores
            """)
        )
        rows = result.fetchall()
        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_demographie", "score_education", "score_sante",
                "score_environnement", "score_transports", "score_risques"]
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

            # Recalculer score_global avec les 9 catégories
            sous_scores = {
                "equipements":    float(row["score_equipements"]) if not pd.isna(row["score_equipements"]) else -1.0,
                "securite":       float(new_score_sec),
                "immobilier":     float(row["score_immobilier"]) if not pd.isna(row["score_immobilier"]) else -1.0,
                "demographie":    float(row["score_demographie"]) if not pd.isna(row["score_demographie"]) else -1.0,
                "education":      float(row["score_education"]) if not pd.isna(row["score_education"]) else -1.0,
                "sante":          float(row["score_sante"]) if not pd.isna(row["score_sante"]) else -1.0,
                "environnement":  float(row["score_environnement"]) if not pd.isna(row["score_environnement"]) else -1.0,
                "transports":     float(row["score_transports"]) if not pd.isna(row["score_transports"]) else -1.0,
                "risques":        float(row["score_risques"]) if not pd.isna(row["score_risques"]) else -1.0,
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
