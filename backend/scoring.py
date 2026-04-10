"""
Logique de calcul des scores de qualité de vie par commune.
Méthode : percentile national → sous-score 0-100 → score global pondéré → lettre A-E
"""

from typing import Optional
import numpy as np
import pandas as pd


# Poids de chaque catégorie — renormalisés automatiquement si catégorie absente
# Calibrage basé sur 14 enquêtes nationales (OpinionWay, IFOP, Ipsos, Guy Hoquet/YouGov,
# ONCV, CREDOC, INSEE, France Armor, Qualitel…) — critères réels des Français.
# Somme = 100%. Le code renormalise quand une catégorie manque (score=-1).
CATEGORIES = {
    "equipements":  {"poids": 0.16, "sens": "direct"},   # commerces/services — top 5 enquêtes (36-50%)
    "immobilier":   {"poids": 0.20, "sens": "inverse"},   # accessibilité prix — #1 critère achat (47-72%)
    "sante":        {"poids": 0.17, "sens": "direct"},   # accès soins — top 5 enquêtes (41-66%)
    "transports":   {"poids": 0.15, "sens": "direct"},   # mobilité — variable urbain/rural (24-47%)
    "securite":     {"poids": 0.13, "sens": "inverse"},  # facteur d'élimination — données SSMSI binaires
    "education":    {"poids": 0.08, "sens": "direct"},   # IPS 40% + DNB 40% + lycée pro 20%
    "environnement":{"poids": 0.07, "sens": "inverse"},  # artificialisation CEREMA + qualité air ATMO
    "risques":      {"poids": 0.04, "sens": "inverse"},  # PPR naturels GASPAR — jamais cité en enquête
    # Catégories retirées :
    # - demographie : autoréférentiel (la croissance est une conséquence, pas un critère de choix)
    # - revenus : biais ségrégant (Saclay 97/100 = riche, pas accessible)
}


def score_to_lettre(score: float) -> str:
    """Convertit un score 0-100 en lettre A-E (arrondi entier pour cohérence affichage)."""
    s = round(score)
    if s >= 80: return "A"
    if s >= 60: return "B"
    if s >= 40: return "C"
    if s >= 20: return "D"
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


def score_immobilier_hybrid(prix: float, all_prix: np.ndarray) -> float:
    """
    Score immobilier hybride P85 : percentile inversé pour les prix <= P85,
    log-linéaire pour les prix > P85 (étale les communes chères).

    En dessous de P85 (~2 700 €/m²) : percentile classique mappé sur 20-100.
    Au-dessus de P85 : log-linéaire mappé sur 0-20.

    Résultat : meilleure différenciation en haut de la distribution
    (Paris 6e ~3.5 vs Rambouillet ~16.6) sans impact sur les communes abordables.
    """
    if prix <= 0 or len(all_prix) == 0:
        return -1.0

    p85 = float(np.percentile(all_prix, 85))

    if prix <= p85:
        # Percentile parmi les communes <= P85, mappé sur 20-100
        below = all_prix[all_prix <= p85]
        rank = float(np.sum(below <= prix)) / len(below)
        return round(20 + (1 - rank) * 80, 1)
    else:
        # Log-linéaire pour le top 15%, mappé sur 0-20
        log_p85 = np.log(p85)
        log_max = np.log(float(all_prix.max()))
        if log_max <= log_p85:
            return 0.0
        frac = (np.log(prix) - log_p85) / (log_max - log_p85)
        return round(max(0, 20 * (1 - frac)), 1)


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

    # Score équipements — hybride présence + densité pondéré par population
    # Petites communes (< ~10k) : dominé par la présence (variété des services)
    # Grandes communes (> ~30k) : dominé par la densité (services par habitant)
    # Transition douce via alpha(pop) = 1 / (1 + (pop/K)^P)
    HYBRID_K = 20000   # point d'inflexion (alpha=0.5 à 20k hab)
    HYBRID_P = 1.5     # pente de la transition

    if "presence_score" in df.columns:
        # 1. Percentile de présence (parmi communes avec présence > 0)
        serie_nz = df["presence_score"][df["presence_score"] > 0]
        df["_pres_pctile"] = df["presence_score"].apply(
            lambda x: 0.0 if pd.isna(x) or x <= 0 else percentile_to_score(x, serie_nz, "direct")
        )

        # 2. Densité pondérée pour 10 000 hab (si weighted_count disponible)
        if "weighted_count" in df.columns and "population" in df.columns:
            df["_density_10k"] = df["weighted_count"] / df["population"].clip(lower=1) * 10000
            dens_nz = df["_density_10k"][df["_density_10k"] > 0]
            df["_dens_pctile"] = df["_density_10k"].apply(
                lambda x: 0.0 if pd.isna(x) or x <= 0 else percentile_to_score(x, dens_nz, "direct")
            )

            # 3. Alpha de transition (1 = 100% présence, 0 = 100% densité)
            df["_alpha"] = df["population"].apply(
                lambda p: 1.0 / (1.0 + (max(p, 1) / HYBRID_K) ** HYBRID_P) if p > 0 else 1.0
            )

            # 4. Score hybride
            df["score_equipements"] = df["_alpha"] * df["_pres_pctile"] + (1 - df["_alpha"]) * df["_dens_pctile"]

            # Nettoyage colonnes temporaires
            df.drop(columns=["_pres_pctile", "_dens_pctile", "_density_10k", "_alpha"], inplace=True)
        else:
            # Fallback : présence seule (pas de weighted_count)
            df["score_equipements"] = df["_pres_pctile"]
            df.drop(columns=["_pres_pctile"], inplace=True)

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
