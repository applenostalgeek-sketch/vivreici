"""
Import données immobilières DVF — prix médian au m² par commune.
Source : geo-dvf (data.gouv.fr) — fichiers CSV par département 2024
         https://files.data.gouv.fr/geo-dvf/latest/csv/2024/departements/

Stratégie :
- Téléchargement des 101 fichiers CSV.gz départementaux (≈1MB chacun)
- Filtrage sur Appartement + Maison avec surface > 9m²
- Calcul du prix/m² par transaction → médiane par commune
- Scoring par percentile national inverse (moins cher = meilleur score)
- Upsert uniquement score_immobilier + prix_m2_median
- Recalcul score_global + lettre pour toutes les communes
"""

import asyncio
import gzip
import io
import sys
import os

import httpx
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, CATEGORIES

# ── Configuration ──────────────────────────────────────────────────────────────

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/2024/departements/"

# Tous les codes départementaux (métropole + DOM)
DEPARTEMENTS = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "2A", "2B",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95",
    "971", "972", "973", "974", "976",
]

# Colonnes nécessaires dans les fichiers DVF
COLONNES = [
    "code_commune",
    "type_local",
    "valeur_fonciere",
    "surface_reelle_bati",
    "nombre_lots",
]

# Types de biens retenus pour le prix au m²
TYPES_LOGEMENT = {"Appartement", "Maison"}

# Seuil minimum de transactions pour considérer la médiane fiable
MIN_TRANSACTIONS = 5

# Concurrence HTTP (download parallèle)
MAX_CONCURRENT = 10


# ── Téléchargement ─────────────────────────────────────────────────────────────

async def telecharger_departement(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    dep: str,
) -> pd.DataFrame | None:
    """Télécharge et parse un fichier CSV.gz départemental DVF."""
    url = f"{BASE_URL}{dep}.csv.gz"
    async with sem:
        try:
            resp = await client.get(url, timeout=120)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(f"  [SKIP] Dept {dep} : HTTP {e.response.status_code}")
            return None
        except Exception as e:
            print(f"  [SKIP] Dept {dep} : {e}")
            return None

    # Décompresser et lire le CSV
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
            df = pd.read_csv(
                gz,
                sep=",",
                dtype={"code_commune": str},
                usecols=lambda c: c in COLONNES,
                low_memory=False,
            )
        return df
    except Exception as e:
        print(f"  [SKIP] Dept {dep} : erreur parsing — {e}")
        return None


async def telecharger_tous_departements() -> pd.DataFrame:
    """Télécharge tous les fichiers DVF en parallèle et les concatène."""
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    print(f"Téléchargement des {len(DEPARTEMENTS)} fichiers DVF 2023...")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = [
            telecharger_departement(client, sem, dep)
            for dep in DEPARTEMENTS
        ]
        resultats = await asyncio.gather(*tasks)

    frames = [df for df in resultats if df is not None and not df.empty]
    print(f"  → {len(frames)} fichiers téléchargés")

    df_all = pd.concat(frames, ignore_index=True)
    print(f"  → {len(df_all):,} lignes totales")
    return df_all


# ── Calcul du prix médian par commune ─────────────────────────────────────────

def calculer_prix_median_par_commune(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    À partir des transactions DVF brutes, calcule le prix médian au m² par commune.

    Filtre :
    - type_local ∈ {Appartement, Maison}
    - valeur_fonciere > 0
    - surface_reelle_bati >= 10 m² (élimine les erreurs)
    - valeur_fonciere / surface < 20 000 €/m² (élimine les aberrations)
    - valeur_fonciere / surface > 200 €/m² (élimine les sous-déclarations)

    Retourne un DataFrame avec colonnes : code_insee, prix_m2_median, nb_transactions
    """
    print("Calcul des prix médians par commune...")

    # Filtrage sur le type de bien
    df = df_raw[df_raw["type_local"].isin(TYPES_LOGEMENT)].copy()
    print(f"  → {len(df):,} transactions logement (appart + maison)")

    # Conversion numérique
    df["valeur_fonciere"] = pd.to_numeric(
        df["valeur_fonciere"].astype(str).str.replace(",", "."), errors="coerce"
    )
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")

    # Filtres qualité
    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati", "code_commune"])
    df = df[df["valeur_fonciere"] > 0]
    df = df[df["surface_reelle_bati"] >= 10]

    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    df = df[(df["prix_m2"] >= 200) & (df["prix_m2"] <= 20_000)]

    # Normaliser le code commune sur 5 caractères
    df["code_commune"] = df["code_commune"].astype(str).str.zfill(5)

    print(f"  → {len(df):,} transactions après filtrage qualité")

    # Médiane par commune
    agg = df.groupby("code_commune").agg(
        prix_m2_median=("prix_m2", "median"),
        nb_transactions=("prix_m2", "count"),
    ).reset_index()

    # Garder uniquement les communes avec suffisamment de transactions
    agg = agg[agg["nb_transactions"] >= MIN_TRANSACTIONS]
    agg = agg.rename(columns={"code_commune": "code_insee"})

    print(f"  → {len(agg):,} communes avec prix médian (>= {MIN_TRANSACTIONS} transactions)")

    # Paris (75101-75120 → 75056), Lyon (69381-69389 → 69123), Marseille (13201-13216 → 13055)
    # Les DVF utilisent les codes arrondissement ; la table communes utilise le code global.
    agg = fusionner_arrondissements(agg, df)

    print(f"  → {len(agg):,} communes après fusion arrondissements PLM")
    return agg


def fusionner_arrondissements(agg: pd.DataFrame, df_transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Paris, Lyon, Marseille : les DVF utilisent les codes arrondissement.
    On conserve les scores par arrondissement (communes en DB) ET on ajoute
    une entrée pour la commune-parent (score global de la ville).

    Paris    : 75101-75120 → conserver + ajouter 75056
    Lyon     : 69381-69389 → conserver + ajouter 69123
    Marseille: 13201-13216 → conserver + ajouter 13055
    """
    PLM = {
        "75056": "751",   # Paris
        "69123": "6938",  # Lyon
        "13055": "1320",  # Marseille
    }

    rows_to_add = []

    for code_parent, prefix in PLM.items():
        # Transactions des arrondissements de cette ville
        mask = df_transactions["code_commune"].astype(str).str.startswith(prefix)
        df_arr = df_transactions[mask].copy()

        if df_arr.empty:
            continue

        if "prix_m2" not in df_arr.columns:
            df_arr["prix_m2"] = df_arr["valeur_fonciere"] / df_arr["surface_reelle_bati"]

        nb = len(df_arr)
        if nb < MIN_TRANSACTIONS:
            continue

        # Entrée parent (score global de la ville)
        rows_to_add.append({
            "code_insee": code_parent,
            "prix_m2_median": round(df_arr["prix_m2"].median(), 2),
            "nb_transactions": nb,
        })

        # Les codes arrondissement (ex. 75108, 69383) sont déjà dans `agg`
        # avec leurs propres médianes — on les conserve tels quels.

    if rows_to_add:
        df_plm = pd.DataFrame(rows_to_add)
        agg = pd.concat([agg, df_plm], ignore_index=True)
        # Dédupliquer : si le parent était déjà dans agg, garder la nouvelle entrée
        agg = agg.drop_duplicates(subset=["code_insee"], keep="last")

    return agg


# ── Scoring par percentile ─────────────────────────────────────────────────────

def calculer_scores_immobilier(df_prix: pd.DataFrame) -> pd.DataFrame:
    """
    Score immobilier = percentile inversé (moins cher = score plus élevé).
    0 = le plus cher nationalement, 100 = le moins cher.
    """
    prix_serie = df_prix["prix_m2_median"]

    # Percentile de chaque commune dans la distribution nationale
    # sens=inverse : rank dans les prix, puis inversé
    df_prix = df_prix.copy()
    df_prix["score_immobilier"] = prix_serie.rank(pct=True).apply(
        lambda p: round((1 - p) * 100, 1)
    )

    return df_prix


# ── Recalcul du score global ───────────────────────────────────────────────────

async def recalculer_scores_globaux(session, df_immo: pd.DataFrame) -> int:
    """
    1. Met à jour score_immobilier + prix_m2_median pour les communes avec données DVF
    2. Recalcule score_global + lettre pour TOUTES les communes
    """

    # Upsert immobilier en batch
    print("Upsert score_immobilier + prix_m2_median...")
    immo_dict = {
        row["code_insee"]: (row["score_immobilier"], row["prix_m2_median"])
        for _, row in df_immo.iterrows()
    }

    count_update = 0
    batch_size = 500
    items = list(immo_dict.items())

    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        for code_insee, (score, prix) in batch:
            await session.execute(
                text("""
                    UPDATE scores
                    SET score_immobilier = :score,
                        prix_m2_median   = :prix
                    WHERE code_insee = :code
                """),
                {"score": score, "prix": prix, "code": code_insee},
            )
        await session.commit()
        count_update += len(batch)
        if count_update % 5000 == 0 or count_update == len(items):
            print(f"  → {count_update}/{len(items)} communes mises à jour")

    print(f"  → {count_update} communes avec score_immobilier mis à jour")

    # Recalcul score_global pour toutes les communes
    print("Recalcul score_global pour toutes les communes...")
    result = await session.execute(
        text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_education, score_sante, score_environnement,
                   score_demographie
            FROM scores
        """)
    )
    rows = result.fetchall()

    cats = list(CATEGORIES.keys())
    cat_cols = [f"score_{cat}" for cat in cats]

    count_global = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        for row in batch:
            code_insee = row[0]
            sous_scores = {}
            for j, cat in enumerate(cats):
                val = row[j + 1]  # +1 car row[0] = code_insee
                if val is not None:
                    sous_scores[cat] = float(val)

            score, lettre, nb = calculer_score_global(sous_scores)

            await session.execute(
                text("""
                    UPDATE scores
                    SET score_global = :score,
                        lettre       = :lettre,
                        nb_categories_scorees = :nb
                    WHERE code_insee = :code
                """),
                {"score": score, "lettre": lettre, "nb": nb, "code": code_insee},
            )
        await session.commit()
        count_global += len(batch)
        if count_global % 10000 == 0:
            print(f"  → {count_global}/{len(rows)} scores globaux recalculés")

    print(f"  → {count_global} scores globaux recalculés")
    return count_update


# ── Exemples de vérification ───────────────────────────────────────────────────

async def afficher_exemples(session):
    """Affiche des exemples pour vérification manuelle."""
    exemples = [
        ("75056", "Paris"),
        ("69123", "Lyon"),
        ("78517", "Rambouillet"),
        ("13055", "Marseille"),
        ("31555", "Toulouse"),
        ("06088", "Nice"),
    ]

    print("\n── Exemples ──────────────────────────────────────────────")
    print(f"{'Commune':<20} {'Code':>6} {'Prix m²':>10} {'Score immo':>12} {'Score global':>14} {'Lettre':>7}")
    print("-" * 75)

    for code, nom in exemples:
        result = await session.execute(
            text("""
                SELECT prix_m2_median, score_immobilier, score_global, lettre
                FROM scores WHERE code_insee = :code
            """),
            {"code": code},
        )
        row = result.fetchone()
        if row:
            prix, score_immo, score_global, lettre = row
            print(
                f"{nom:<20} {code:>6} "
                f"{prix:>9.0f}€ {score_immo:>11.1f} "
                f"{score_global:>13.1f} {lettre:>7}"
            )
        else:
            print(f"{nom:<20} {code:>6}  (non trouvé)")

    print()

    # Stats globales
    result = await session.execute(
        text("""
            SELECT
                COUNT(*) FILTER (WHERE score_immobilier >= 0) as nb_scorees,
                AVG(prix_m2_median) FILTER (WHERE prix_m2_median > 0) as prix_moyen,
                MIN(prix_m2_median) FILTER (WHERE prix_m2_median > 0) as prix_min,
                MAX(prix_m2_median) FILTER (WHERE prix_m2_median > 0) as prix_max
            FROM scores
        """)
    )
    stats = result.fetchone()
    print(f"Communes avec score_immobilier : {stats[0]:,}")
    print(f"Prix m² moyen national         : {stats[1]:.0f} €")
    print(f"Prix m² min                    : {stats[2]:.0f} €")
    print(f"Prix m² max                    : {stats[3]:.0f} €")


# ── Point d'entrée ─────────────────────────────────────────────────────────────

async def run():
    await init_db()

    # 1. Télécharger tous les fichiers DVF
    df_raw = await telecharger_tous_departements()

    # 2. Calculer le prix médian au m² par commune
    df_prix = calculer_prix_median_par_commune(df_raw)

    # 3. Calculer le score immobilier (percentile inversé)
    df_scored = calculer_scores_immobilier(df_prix)

    # 4. Upsert en base + recalcul score_global
    async with async_session() as session:
        nb = await recalculer_scores_globaux(session, df_scored)
        await afficher_exemples(session)

    print(f"\nImport DVF terminé — {nb} communes scorées.")


if __name__ == "__main__":
    asyncio.run(run())
