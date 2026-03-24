"""
Scoring IRIS des équipements et services (BPE 2024).
Utilise la colonne DCIRIS du fichier BPE (code commune 5 chars + code IRIS 4 chars = 9 chars).
Score percentile national sur TOUS les IRIS.
"""

import asyncio
import httpx
import pandas as pd
import io
import json
import sys
import os
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import percentile_to_score, calculer_score_global, normaliser_par_habitant
from backend.data_import.import_bpe import (
    BPE_URL, EQUIPEMENTS_SELECTIONNES, CODES_MEDECINS
)


async def charger_bpe() -> pd.DataFrame:
    """Télécharge le fichier BPE et le retourne sous forme de DataFrame."""
    print("Téléchargement BPE 2024...")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(BPE_URL, timeout=300)
        resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        with z.open(csv_files[0]) as f:
            df = pd.read_csv(f, sep=";",
                             dtype={"DEPCOM": str, "TYPEQU": str, "DCIRIS": str},
                             low_memory=False)
    print(f"  → {len(df):,} équipements chargés")
    return df


def aggreger_par_iris(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège les équipements par IRIS."""
    # Filtrer les équipements retenus
    codes_voulus = set(EQUIPEMENTS_SELECTIONNES.keys())
    df_filtre = df[df["TYPEQU"].isin(codes_voulus)].copy()

    # S'assurer que DCIRIS est disponible et valide
    if "DCIRIS" not in df_filtre.columns:
        raise ValueError("Colonne DCIRIS absente du fichier BPE")

    df_filtre = df_filtre.dropna(subset=["DCIRIS"])
    df_filtre = df_filtre[df_filtre["DCIRIS"].str.len() == 9]

    print(f"  → {len(df_filtre):,} équipements avec code IRIS valide")

    # Compter par IRIS et type
    pivot = df_filtre.groupby(["DCIRIS", "TYPEQU"]).size().unstack(fill_value=0)
    pivot.columns.name = None
    pivot = pivot.reset_index().rename(columns={"DCIRIS": "code_iris"})

    pivot["nb_equipements"] = pivot[
        [c for c in pivot.columns if c != "code_iris"]
    ].sum(axis=1)
    pivot["nb_medecins"] = pivot[[c for c in CODES_MEDECINS if c in pivot.columns]].sum(axis=1)

    # Détail par type (même logique que import_bpe.py)
    label_map = {k: v for k, v in EQUIPEMENTS_SELECTIONNES.items() if k in pivot.columns}
    def build_detail(row):
        d = {label_map[code]: int(row[code]) for code in label_map if int(row.get(code, 0)) > 0}
        return json.dumps(d, ensure_ascii=False) if d else None
    pivot["equipements_detail"] = pivot.apply(build_detail, axis=1)

    return pivot[["code_iris", "nb_equipements", "nb_medecins", "equipements_detail"]]


async def run():
    await init_db()

    df_bpe = await charger_bpe()

    print("Agrégation par IRIS...")
    df_iris = aggreger_par_iris(df_bpe)

    # Charger toutes les zones IRIS depuis la base pour le scoring national
    async with async_session() as session:
        result = await session.execute(text(
            "SELECT code_iris, population FROM iris_zones"
        ))
        df_zones = pd.DataFrame(result.fetchall(), columns=["code_iris", "population"])

    print(f"  → {len(df_zones):,} zones IRIS en base")

    df = df_zones.merge(df_iris, on="code_iris", how="left")
    df["nb_equipements"]    = df["nb_equipements"].fillna(0).astype(int)
    df["nb_medecins"]       = df["nb_medecins"].fillna(0).astype(int)
    df["population"]        = df["population"].fillna(0).astype(int)
    df["equipements_detail"] = df["equipements_detail"].where(df["equipements_detail"].notna(), None)

    # Scoring par percentile national sur les IRIS avec données BPE.
    # Les IRIS sont conçus pour avoir ~2000 habitants chacun → compter direct sans normalisation.
    # Pour nb_medecins : les zones sans médecin couvrent les IRIS ruraux ; les grandes villes
    # ont plusieurs IRIS pour couvrir les mêmes médecins → nb_medecins direct acceptable.
    print("Calcul des scores percentiles IRIS...")

    df_avec = df[df["nb_equipements"] > 0]  # Score uniquement les IRIS avec équipements
    serie_eq  = df_avec["nb_equipements"].astype(float)
    serie_med = df_avec["nb_medecins"].astype(float)

    df["score_equipements"] = df["nb_equipements"].astype(float).apply(
        lambda x: percentile_to_score(x, serie_eq, "direct") if x > 0 else -1
    )
    df["score_sante"] = df["nb_medecins"].astype(float).apply(
        lambda x: percentile_to_score(x, serie_med, "direct") if x >= 0 else -1
    )

    # Métriques pour les données brutes (estimation pour affichage)
    POP_IRIS_MOYENNE = 2000
    df["medecins_pour_10000"] = df["nb_medecins"].apply(
        lambda x: round(x / POP_IRIS_MOYENNE * 10000, 2)
    )

    # Upsert en base
    print("Sauvegarde en base (iris_scores)...")
    async with async_session() as session:
        count = 0
        for _, row in df.iterrows():
            if row["nb_equipements"] == 0 and row["nb_medecins"] == 0:
                continue  # Ne pas scorer les IRIS sans équipement

            await session.execute(text("""
                INSERT INTO iris_scores (
                    code_iris, score_global, lettre,
                    score_equipements, score_sante, score_immobilier, score_revenus,
                    nb_equipements, nb_medecins_pour_10000,
                    prix_m2_median, revenu_median, taux_pauvrete,
                    equipements_detail, nb_categories_scorees, updated_at
                ) VALUES (
                    :code, 50, 'C',
                    :seq, :sante, -1, -1,
                    :nb_eq, :nb_med,
                    0, 0, 0,
                    :detail, :nb_cat, CURRENT_TIMESTAMP
                )
                ON CONFLICT(code_iris) DO UPDATE SET
                    score_equipements      = excluded.score_equipements,
                    score_sante            = excluded.score_sante,
                    nb_equipements         = excluded.nb_equipements,
                    nb_medecins_pour_10000 = excluded.nb_medecins_pour_10000,
                    equipements_detail     = excluded.equipements_detail,
                    updated_at             = excluded.updated_at
            """), {
                "code":   row["code_iris"],
                "seq":    float(row["score_equipements"]),
                "sante":  float(row["score_sante"]),
                "nb_eq":  int(row["nb_equipements"]),
                "nb_med": float(row.get("medecins_pour_10000", 0)),
                "nb_cat": 2,
                "detail": row.get("equipements_detail") or None,
            })
            count += 1
            if count % 2000 == 0:
                await session.commit()
                print(f"  → {count} IRIS scorés (équipements/santé)")

        await session.commit()

    print(f"  → {count} IRIS scorés avec données BPE.")
    await recalculer_scores_globaux()


async def recalculer_scores_globaux():
    """Recalcule score_global en tenant compte de tous les sous-scores disponibles."""
    from backend.scoring import calculer_score_global as calc

    print("Recalcul scores globaux IRIS...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_iris,
                   score_equipements, score_sante, score_immobilier, score_revenus
            FROM iris_scores
        """))
        rows = result.fetchall()

        nb = 0
        for row in rows:
            code, seq, sante, simmo, srev = row
            sous_scores = {}
            if seq is not None and seq >= 0:    sous_scores["equipements"] = seq
            if sante is not None and sante >= 0: sous_scores["sante"] = sante
            if simmo is not None and simmo >= 0: sous_scores["immobilier"] = simmo
            if srev is not None and srev >= 0:   sous_scores["revenus"] = srev
            if not sous_scores:
                continue
            score, lettre, nb_cat = calc(sous_scores)
            # Exiger au moins 2 catégories pour attribuer une lettre fiable.
            # Avec 1 seule catégorie (ex: revenus seuls), le score est artificiel
            # car les catégories manquantes (ex: immobilier Paris = E) ne tirent pas vers le bas.
            # '?' = sentinel "données partielles" (la colonne est NOT NULL en base)
            if nb_cat < 2:
                lettre = '?'
            await session.execute(text("""
                UPDATE iris_scores
                SET score_global = :sg, lettre = :l, nb_categories_scorees = :nb
                WHERE code_iris = :c
            """), {"sg": score, "l": lettre, "nb": nb_cat, "c": code})
            nb += 1
            if nb % 5000 == 0:
                await session.commit()

        await session.commit()
    print(f"  → {nb} scores globaux IRIS recalculés.")


if __name__ == "__main__":
    asyncio.run(run())
