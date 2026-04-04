"""
Recalcule le score_equipements avec la méthode hybride (présence + densité)
à partir des données equipements_detail déjà en base.
Ne re-télécharge PAS le BPE — utilise les données existantes.

Usage : python3 -m scripts.recalc_equipements_hybride
"""

import asyncio
import json
import sys
import os

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score

# Mêmes poids que import_bpe.py
POIDS_PRESENCE = {
    "supermarché": 4, "hypermarché": 4, "boulangerie": 2, "boucherie": 1,
    "pharmacie": 4, "hôpital": 2, "urgences": 2, "bureau_poste": 2,
    "gymnase": 1, "piscine": 1, "cinéma": 1, "bibliothèque": 1, "théâtre": 1,
}

# Paramètres hybride (identiques à scoring.py)
HYBRID_K = 20000
HYBRID_P = 1.5


def calc_from_detail(detail_json):
    """Calcule presence_score et weighted_count depuis equipements_detail JSON."""
    if not detail_json:
        return 0, 0
    try:
        d = json.loads(detail_json)
        presence = sum(POIDS_PRESENCE.get(k, 0) * min(v, 1) for k, v in d.items() if POIDS_PRESENCE.get(k, 0) > 0)
        weighted = sum(POIDS_PRESENCE.get(k, 0) * v for k, v in d.items() if POIDS_PRESENCE.get(k, 0) > 0)
        return presence, weighted
    except Exception:
        return 0, 0


async def run():
    await init_db()

    # Charger toutes les communes avec scores et détail équipements
    print("Chargement des données...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT c.code_insee, c.nom, c.population,
                   s.score_equipements, s.score_securite, s.score_immobilier,
                   s.score_education, s.score_sante, s.score_environnement,
                   s.score_demographie, s.score_transports, s.score_risques,
                   s.score_global, s.lettre, s.nb_categories_scorees,
                   s.equipements_detail
            FROM communes c
            JOIN scores s ON c.code_insee = s.code_insee
        """))
        rows = result.fetchall()
        cols = [
            "code_insee", "nom", "population",
            "score_equipements", "score_securite", "score_immobilier",
            "score_education", "score_sante", "score_environnement",
            "score_demographie", "score_transports", "score_risques",
            "score_global", "lettre", "nb_categories_scorees",
            "equipements_detail",
        ]
        df = pd.DataFrame(rows, columns=cols)

    print(f"  {len(df)} communes chargées")

    # Calcul presence_score et weighted_count depuis equipements_detail
    results = df["equipements_detail"].apply(calc_from_detail)
    df["presence_score"] = results.apply(lambda x: x[0])
    df["weighted_count"] = results.apply(lambda x: x[1])

    print(f"  Communes avec équipements: {(df['presence_score'] > 0).sum()}")

    # === SCORE HYBRIDE ===

    # 1. Percentile de présence (parmi non-zéro)
    serie_nz = df["presence_score"][df["presence_score"] > 0]
    df["pres_pctile"] = df["presence_score"].apply(
        lambda x: 0.0 if pd.isna(x) or x <= 0 else percentile_to_score(x, serie_nz, "direct")
    )

    # 2. Densité pour 10 000 hab
    df["density_10k"] = df["weighted_count"] / df["population"].clip(lower=1) * 10000
    dens_nz = df["density_10k"][df["density_10k"] > 0]
    df["dens_pctile"] = df["density_10k"].apply(
        lambda x: 0.0 if pd.isna(x) or x <= 0 else percentile_to_score(x, dens_nz, "direct")
    )

    # 3. Alpha de transition
    df["alpha"] = df["population"].apply(
        lambda p: 1.0 / (1.0 + (max(p, 1) / HYBRID_K) ** HYBRID_P) if p > 0 else 1.0
    )

    # 4. Score hybride
    df["new_score_eq"] = df["alpha"] * df["pres_pctile"] + (1 - df["alpha"]) * df["dens_pctile"]

    # === RECALCUL SCORE GLOBAL ===
    cat_map = {
        "equipements": "new_score_eq",
        "securite": "score_securite",
        "immobilier": "score_immobilier",
        "education": "score_education",
        "sante": "score_sante",
        "environnement": "score_environnement",
        "demographie": "score_demographie",
        "transports": "score_transports",
        "risques": "score_risques",
    }

    def calc_new_global(row):
        sous_scores = {}
        for cat, col in cat_map.items():
            val = row[col]
            if val is not None and not pd.isna(val) and val >= 0:
                sous_scores[cat] = float(val)
        return calculer_score_global(sous_scores)

    new_globals = df.apply(calc_new_global, axis=1)
    df["new_global"] = new_globals.apply(lambda x: x[0])
    df["new_lettre"] = new_globals.apply(lambda x: x[1])
    df["new_nb_cat"] = new_globals.apply(lambda x: x[2])

    # === DIAGNOSTIC ===
    changed = df[df["lettre"] != df["new_lettre"]]
    print(f"\n=== RÉSULTATS ===")
    print(f"Communes changeant de lettre: {len(changed)} / {len(df)} ({len(changed)/len(df)*100:.1f}%)")

    up = (changed.apply(lambda r: "ABCDE".index(r["new_lettre"]) < "ABCDE".index(r["lettre"]), axis=1)).sum()
    dn = len(changed) - up
    print(f"  Montées: {up}, Descentes: {dn}")

    print(f"\nDistribution:")
    for l in ["A", "B", "C", "D", "E"]:
        c0 = (df["lettre"] == l).sum()
        cn = (df["new_lettre"] == l).sum()
        print(f"  {l}: {c0:>6} → {cn:>6} ({cn - c0:>+5})")

    # Villes clés
    print(f"\n{'Commune':<25} {'Pop':>7} {'α':>5} {'PresPct':>7} {'DensPct':>7} {'HybEq':>6} | {'Avant':>7} → {'Après':>7}")
    print("-" * 90)
    test_codes = [
        "75101", "75105", "75111", "75115", "75120",
        "92012", "92050", "93066",
        "31555", "44109", "33063", "06088", "59350",
        "78517", "55029", "15187", "23096",
        "64445", "81004", "12202", "01004", "36044",
    ]
    for code in test_codes:
        row = df[df["code_insee"] == code]
        if row.empty:
            continue
        r = row.iloc[0]
        mark = " ←" if r["lettre"] != r["new_lettre"] else ""
        print(
            f'{r["nom"]:<25} {r["population"]:>7} {r["alpha"]:>5.2f} '
            f'{r["pres_pctile"]:>7.0f} {r["dens_pctile"]:>7.0f} {r["new_score_eq"]:>6.0f} | '
            f'{r["score_global"]:>5.0f}{r["lettre"]} → {r["new_global"]:>5.0f}{r["new_lettre"]}{mark}'
        )

    # Quelques villages
    print("-" * 90)
    vils = df[(df["population"] > 200) & (df["population"] < 800) & (df["presence_score"] > 0)].sample(5, random_state=42)
    for _, r in vils.iterrows():
        mark = " ←" if r["lettre"] != r["new_lettre"] else ""
        print(
            f'{r["nom"]:<25} {r["population"]:>7} {r["alpha"]:>5.2f} '
            f'{r["pres_pctile"]:>7.0f} {r["dens_pctile"]:>7.0f} {r["new_score_eq"]:>6.0f} | '
            f'{r["score_global"]:>5.0f}{r["lettre"]} → {r["new_global"]:>5.0f}{r["new_lettre"]}{mark}'
        )

    # === SAUVEGARDE ===
    confirm = input("\nAppliquer les changements en base ? (oui/non) : ")
    if confirm.strip().lower() != "oui":
        print("Annulé.")
        return

    print("Mise à jour en base...")
    async with async_session() as session:
        count = 0
        for _, row in df.iterrows():
            await session.execute(text("""
                UPDATE scores
                SET score_equipements = :seq,
                    score_global = :sg,
                    lettre = :l,
                    nb_categories_scorees = :nb,
                    updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :c
            """), {
                "seq": round(float(row["new_score_eq"]), 1),
                "sg": float(row["new_global"]),
                "l": row["new_lettre"],
                "nb": int(row["new_nb_cat"]),
                "c": row["code_insee"],
            })
            count += 1
            if count % 5000 == 0:
                await session.commit()
                print(f"  {count}/{len(df)}...")
        await session.commit()

    print(f"  {count} communes mises à jour.")
    print("Terminé. Relancer export_all_static.py pour régénérer les JSON.")


if __name__ == "__main__":
    asyncio.run(run())
