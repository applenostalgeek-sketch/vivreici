"""
Logique de calcul des scores de qualité de vie par commune.
Méthode : percentile national → sous-score 0-100 → score global pondéré → lettre A-E
"""

from typing import Optional
import pandas as pd


# Poids de chaque catégorie — renormalisés automatiquement si catégorie absente
# Logique : score par défaut calibré pour l'actif en mobilité (profil majoritaire)
CATEGORIES = {
    "equipements":  {"poids": 0.20, "sens": "direct"},   # critère quotidien, données BPE fiables
    "transports":   {"poids": 0.18, "sens": "direct"},   # critère décisif hors métropole
    "sante":        {"poids": 0.18, "sens": "direct"},   # APL DREES — accessibilité aire de chalandise
    "securite":     {"poids": 0.14, "sens": "inverse"},  # facteur d'élimination, données SSMSI biaisées
    "immobilier":   {"poids": 0.14, "sens": "inverse"},  # accessibilité logement
    "education":    {"poids": 0.08, "sens": "direct"},   # IPS 40% + DNB 40% + lycée pro 20%
    "environnement":{"poids": 0.08, "sens": "inverse"},  # espaces non-artificialisés CEREMA 2021
    "demographie":  {"poids": 0.04, "sens": "direct"},   # évolution population 2016→2021
    # NB : le code renormalise automatiquement quand une catégorie manque (score=-1).
    # Cohésion (revenus) retirée : taux pauvreté = proxy richesse, biais ségrégant (ex: Saclay 97/100).
}


def score_to_lettre(score: float) -> str:
    """Convertit un score 0-100 en lettre A-E."""
    if score >= 80: return "A"
    if score >= 60: return "B"
    if score >= 40: return "C"
    if score >= 20: return "D"
    return "E"


def percentile_to_score(valeur: float, serie: pd.Series, sens: str = "direct") -> float:
    """
    Calcule le sous-score (0-100) d'une commune basé sur son percentile national.
    sens='direct' : plus la valeur est haute, meilleur est le score
    sens='inverse' : plus la valeur est basse, meilleur est le score
    """
    if serie.empty or pd.isna(valeur):
        return -1.0

    percentile = (serie < valeur).mean() * 100  # percentile de 0 à 100

    if sens == "inverse":
        return 100 - percentile
    return percentile


def calculer_score_global(sous_scores: dict[str, float]) -> tuple[float, str, int]:
    """
    Calcule le score global à partir des sous-scores.
    Ignore les catégories avec sous-score = -1 (données manquantes).
    Retourne (score_global, lettre, nb_categories_scorees)
    """
    scores_disponibles = {
        cat: score for cat, score in sous_scores.items()
        if score >= 0 and cat in CATEGORIES
    }

    if not scores_disponibles:
        return 50.0, "C", 0

    # Recalculer les poids en excluant les catégories manquantes
    poids_total = sum(CATEGORIES[cat]["poids"] for cat in scores_disponibles)

    score = sum(
        sous_scores[cat] * (CATEGORIES[cat]["poids"] / poids_total)
        for cat in scores_disponibles
    )

    score_rounded = round(score, 1)
    return score_rounded, score_to_lettre(score_rounded), len(scores_disponibles)


def normaliser_par_habitant(valeur: float, population: int, pour: int = 1000) -> float:
    """Normalise une valeur par habitant (ex: équipements pour 1000 hab)."""
    if population <= 0:
        return 0.0
    return (valeur / population) * pour


def calculer_scores_batch(df_communes: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les scores pour toutes les communes en batch.
    df_communes doit avoir les colonnes: code_insee + indicateurs bruts
    Retourne le DataFrame avec les scores ajoutés.
    """
    df = df_communes.copy()

    # Score équipements (si disponible)
    if "equipements_pour_1000" in df.columns:
        serie = df["equipements_pour_1000"].dropna()
        df["score_equipements"] = df["equipements_pour_1000"].apply(
            lambda x: percentile_to_score(x, serie, "direct")
        )

    # Score sécurité (si disponible)
    if "taux_criminalite" in df.columns:
        serie = df["taux_criminalite"].dropna()
        df["score_securite"] = df["taux_criminalite"].apply(
            lambda x: percentile_to_score(x, serie, "inverse")
        )

    # Score démographie (si disponible)
    if "evolution_population_5ans" in df.columns:
        serie = df["evolution_population_5ans"].dropna()
        df["score_demographie"] = df["evolution_population_5ans"].apply(
            lambda x: percentile_to_score(x, serie, "direct")
        )

    # Score santé (si disponible)
    if "medecins_pour_10000" in df.columns:
        serie = df["medecins_pour_10000"].dropna()
        df["score_sante"] = df["medecins_pour_10000"].apply(
            lambda x: percentile_to_score(x, serie, "direct")
        )

    # Calculer le score global pour chaque commune
    cat_cols = {
        "equipements": "score_equipements",
        "securite": "score_securite",
        "demographie": "score_demographie",
        "sante": "score_sante",
    }

    def calc_global(row):
        sous_scores = {}
        for cat, col in cat_cols.items():
            if col in row.index:
                sous_scores[cat] = row[col]
        score, lettre, nb = calculer_score_global(sous_scores)
        return pd.Series({"score_global": score, "lettre": lettre, "nb_categories": nb})

    df[["score_global", "lettre", "nb_categories"]] = df.apply(calc_global, axis=1)

    return df
