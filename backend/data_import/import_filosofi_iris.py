"""
Scoring IRIS des revenus et pauvreté (Filosofi 2021 — INSEE, dernier millésime).
Source : https://www.insee.fr/fr/statistiques/8229323
"""

import asyncio
import httpx
import pandas as pd
import io
import sys
import os
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import percentile_to_score, calculer_score_global

# Revenus disponibles au niveau IRIS (communes >= 5000 hab) — 2021 = dernier millésime
FILOSOFI_IRIS_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/8229323/BASE_TD_FILO_IRIS_2021_DISP_CSV.zip"
)


async def telecharger_filosofi_iris() -> pd.DataFrame:
    print("Téléchargement Filosofi IRIS 2021 — revenus disponibles (INSEE)...")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(FILOSOFI_IRIS_URL, timeout=300)
        resp.raise_for_status()

    print(f"  → Reçu ({len(resp.content) / 1024:.0f} Ko)")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv") and not f.startswith("meta_")]
        if not csv_files:
            print(f"  Fichiers disponibles : {z.namelist()}")
            raise ValueError("Fichier CSV introuvable dans l'archive Filosofi IRIS")

        csv_file = csv_files[0]
        print(f"  → Lecture de {csv_file}")
        with z.open(csv_file) as f:
            df = pd.read_csv(f, sep=";", dtype=str)

    print(f"  → {len(df):,} lignes IRIS chargées")
    print(f"  → Colonnes : {list(df.columns[:15])}")
    return df


def preparer_donnees(df: pd.DataFrame) -> pd.DataFrame:
    """Extrait et nettoie les colonnes revenus/pauvreté."""
    cols_upper = {c.upper(): c for c in df.columns}

    # Code IRIS : peut être colonne "IRIS" (9 chars) ou construire depuis COM+IRIS
    if "IRIS" in cols_upper and len(df[cols_upper["IRIS"]].dropna().iloc[0].strip()) == 9:
        df["code_iris"] = df[cols_upper["IRIS"]].str.strip()
    elif "IRIS" in cols_upper and "COM" in cols_upper:
        df["code_iris"] = df[cols_upper["COM"]].str.zfill(5) + df[cols_upper["IRIS"]].str.zfill(4)
    elif "CODGEO" in cols_upper:
        df["code_iris"] = df[cols_upper["CODGEO"]].str.strip()
    else:
        raise ValueError(f"Code IRIS introuvable. Disponibles : {list(df.columns)}")

    # Colonnes revenus — chercher MED21 et TP6021 (avec ou sans préfixe DISP_/DEC_)
    med_col  = None
    pauv_col = None
    for c_upper, c in cols_upper.items():
        if c_upper.endswith("MED21"):
            med_col = c
        if c_upper.endswith("TP6021"):
            pauv_col = c

    if not med_col:
        raise ValueError(f"Colonne MED20 introuvable. Disponibles : {list(df.columns)}")

    print(f"  → Colonnes utilisées : revenu={med_col}, pauvreté={pauv_col}")

    df_out = pd.DataFrame()
    df_out["code_iris"] = df["code_iris"].str.strip()

    # Nettoyer les valeurs numériques (remplacer 's' = secret statistique par NaN)
    def parse_num(series):
        return pd.to_numeric(series.str.replace(",", ".").replace("s", None), errors="coerce")

    if med_col:
        df_out["revenu_median"] = parse_num(df[med_col])
    if pauv_col:
        df_out["taux_pauvrete"] = parse_num(df[pauv_col])

    # Garder uniquement les lignes avec au moins une valeur
    df_out = df_out.dropna(subset=["revenu_median", "taux_pauvrete"], how="all")

    # Filtrer les codes IRIS de 9 caractères valides
    df_out = df_out[df_out["code_iris"].str.len() == 9]

    print(f"  → {len(df_out):,} IRIS avec données revenus/pauvreté")
    return df_out


async def run():
    await init_db()

    df_raw = await telecharger_filosofi_iris()
    df = preparer_donnees(df_raw)

    # Scoring percentile sur tous les IRIS avec données
    print("Calcul des scores percentiles IRIS revenus...")

    df_valide = df.dropna(subset=["revenu_median"])
    serie_revenu = df_valide["revenu_median"]
    serie_pauv   = df_valide["taux_pauvrete"].dropna()

    # Score revenus = taux_pauvrete seul (inversé) — cohérent avec le scoring communes
    # Le revenu médian n'entre pas dans le score (biais ségrégant), conservé pour info uniquement
    df["score_pauvrete"] = df["taux_pauvrete"].apply(
        lambda x: percentile_to_score(x, serie_pauv, "inverse") if pd.notna(x) else -1
    )

    df["score_revenus"] = df["score_pauvrete"]
    df = df[df["score_revenus"] >= 0]

    print(f"  → {len(df):,} IRIS avec score revenus")

    # Upsert en base
    print("Sauvegarde en base...")
    async with async_session() as session:
        count = 0
        for _, row in df.iterrows():
            await session.execute(text("""
                INSERT INTO iris_scores (
                    code_iris, score_global, lettre,
                    score_equipements, score_sante, score_immobilier, score_revenus,
                    nb_equipements, nb_medecins_pour_10000,
                    prix_m2_median, revenu_median, taux_pauvrete,
                    nb_categories_scorees, updated_at
                ) VALUES (
                    :code, 50, 'C',
                    -1, -1, -1, :srev,
                    0, 0,
                    0, :rev_med, :taux_pauv,
                    1, CURRENT_TIMESTAMP
                )
                ON CONFLICT(code_iris) DO UPDATE SET
                    score_revenus  = excluded.score_revenus,
                    revenu_median  = excluded.revenu_median,
                    taux_pauvrete  = excluded.taux_pauvrete,
                    updated_at     = excluded.updated_at
            """), {
                "code":      row["code_iris"],
                "srev":      float(row["score_revenus"]),
                "rev_med":   float(row["revenu_median"]) if pd.notna(row.get("revenu_median")) else 0,
                "taux_pauv": float(row["taux_pauvrete"]) if pd.notna(row.get("taux_pauvrete")) else 0,
            })
            count += 1
            if count % 3000 == 0:
                await session.commit()
                print(f"  → {count}/{len(df)} IRIS revenus sauvegardés")

        await session.commit()

    print(f"  → {count} IRIS scorés revenus.")
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
