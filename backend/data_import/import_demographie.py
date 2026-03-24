"""
Import populations légales INSEE — évolution démographique par commune.

Sources :
  - Populations légales 2016 : XLS (INSEE, fichier ensemble.xls)
  - Populations légales 2021 : ZIP contenant XLSX (INSEE, fichier ensemble.zip)

Métrique : évolution_pop_5ans = (pop2021 - pop2016) / pop2016 × 100
Score    : percentile national direct (croissance = meilleur)

Lancement :
  cd /Users/admin/vivreici && .venv/bin/python -m backend.data_import.import_demographie
"""

import asyncio
import sys
import os
import io
import zipfile
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
import pandas as pd
import numpy as np

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score


# ── URLs INSEE ─────────────────────────────────────────────────────────────────

URL_2016 = "https://www.insee.fr/fr/statistiques/fichier/3677785/ensemble.xls"
URL_2021 = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"


# ── Téléchargement ─────────────────────────────────────────────────────────────

async def telecharger(url: str, label: str) -> bytes:
    print(f"Téléchargement {label}...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=120) as client:
        resp = await client.get(url)
        resp.raise_for_status()
    print(f"  → {len(resp.content) // 1024:,} Ko")
    return resp.content


# ── Extraction des populations par commune ─────────────────────────────────────

def _df_vers_pop(df: pd.DataFrame, annee: int) -> pd.DataFrame:
    """
    Extrait code_insee et pop depuis un DataFrame déjà chargé (CSV ou Excel).
    Gère les deux conventions INSEE :
      1. Colonne CODGEO (5 chiffres directement)
      2. Colonnes séparées "Code département" + "Code commune"
    """
    cols_lower = {str(c).lower().strip(): str(c) for c in df.columns}

    def find(*keywords):
        for kw in keywords:
            for col_l, col_orig in cols_lower.items():
                if kw in col_l:
                    return col_orig
        return None

    col_codgeo = find("codgeo")
    col_dept   = find("code département", "codedepartement", "code_dep", "dep")
    col_com    = find("codcom", "code commune", "codecommune", "code_com")
    col_pop    = find("population municipale", "pmunicip", "pmun", "psdc", "ptot")

    print(f"  Colonnes : codgeo={col_codgeo}, dept={col_dept}, com={col_com}, pop={col_pop}")

    if col_pop is None:
        raise ValueError(f"Colonne population introuvable. Colonnes : {list(df.columns)}")

    result = pd.DataFrame()
    if col_codgeo:
        result["code_insee"] = df[col_codgeo].astype(str).str.strip().str.zfill(5)
    elif col_dept and col_com:
        dept = df[col_dept].astype(str).str.strip().str.zfill(2)
        com  = df[col_com].astype(str).str.strip().str.zfill(3)
        result["code_insee"] = dept + com
    else:
        raise ValueError(f"Code commune introuvable. Colonnes : {list(df.columns)}")

    result["pop"] = pd.to_numeric(df[col_pop], errors="coerce")
    result = result[result["code_insee"].str.match(r"^\d{5}$")]
    result = result.dropna(subset=["pop"])
    result = result[result["pop"] > 0]
    result["pop"] = result["pop"].astype(int)
    result = result.drop_duplicates(subset="code_insee", keep="first")
    print(f"  → {len(result):,} communes avec population {annee}")
    return result.set_index("code_insee")


def extraire_pop(data: bytes, annee: int, est_zip: bool = False) -> pd.DataFrame:
    """
    Lit un fichier INSEE populations légales (XLS, ZIP+CSV, ZIP+Excel).
    Retourne un DataFrame indexé par code_insee avec colonne pop.
    """
    # Extraire le fichier communes si ZIP
    if est_zip:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            noms = zf.namelist()
            print(f"  Fichiers dans le ZIP : {noms}")
            # Priorité : fichier CSV contenant "commune"
            cibles_csv = [n for n in noms if "commune" in n.lower() and n.endswith(".csv")]
            cibles_xls = [n for n in noms if "commune" in n.lower() and n.endswith((".xls", ".xlsx"))]
            cibles_any_csv = [n for n in noms if n.endswith(".csv") and "commune" in n.lower()]
            nom_cible = (cibles_csv or cibles_xls or cibles_any_csv or [None])[0]
            if nom_cible is None:
                raise ValueError(f"Aucun fichier communes trouvé dans le ZIP : {noms}")
            print(f"  Fichier sélectionné : {nom_cible}")
            data = zf.read(nom_cible)
            # Forcer le type basé sur l'extension
            est_zip = False
            if nom_cible.endswith(".csv"):
                return _lire_csv(data, annee)

    # Détecter CSV vs Excel d'après les premiers octets
    # Excel binaire (.xls) commence par D0 CF, Excel (.xlsx) par PK (ZIP)
    if data[:2] in [b'\xd0\xcf', b'PK']:
        return _lire_excel(data, annee)
    else:
        return _lire_csv(data, annee)


def _lire_csv(data: bytes, annee: int) -> pd.DataFrame:
    print("  Format : CSV")
    df = None
    for sep in [";", ","]:
        for enc in ["utf-8", "latin-1"]:
            try:
                df_tmp = pd.read_csv(io.BytesIO(data), sep=sep, dtype=str,
                                     on_bad_lines="skip", encoding=enc)
                if len(df_tmp.columns) >= 4:
                    df = df_tmp
                    break
            except Exception:
                continue
        if df is not None:
            break
    if df is None:
        raise ValueError("Impossible de lire le CSV INSEE")
    return _df_vers_pop(df, annee)


def _lire_excel(data: bytes, annee: int) -> pd.DataFrame:
    print("  Format : Excel")
    xl = pd.ExcelFile(io.BytesIO(data))
    print(f"  Feuilles : {xl.sheet_names}")

    feuille = next(
        (n for n in xl.sheet_names if "commune" in n.lower()),
        xl.sheet_names[0]
    )
    print(f"  Feuille sélectionnée : {feuille}")

    # Trouver la ligne d'en-tête (contient "municipale" ou "codgeo" mais pas en titre général)
    df_raw = pd.read_excel(io.BytesIO(data), sheet_name=feuille, header=None, dtype=str)
    header_row = None
    for i, row in df_raw.iterrows():
        vals = [str(v).lower() for v in row.values if pd.notna(v)]
        nb = sum(1 for v in row.values if pd.notna(v) and str(v).strip())
        if nb >= 3 and any("municipale" in v or "codgeo" in v for v in vals):
            header_row = i
            break
    if header_row is None:
        raise ValueError("Ligne d'en-tête introuvable dans le fichier Excel INSEE")
    print(f"  Ligne d'en-tête : {header_row}")

    df = pd.read_excel(io.BytesIO(data), sheet_name=feuille, header=header_row, dtype=str)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    return _df_vers_pop(df, annee)


# ── Calcul de l'évolution ──────────────────────────────────────────────────────

def calculer_evolution(df_2016: pd.DataFrame, df_2021: pd.DataFrame) -> pd.DataFrame:
    """
    Joint les deux séries de population et calcule l'évolution en %.
    Retourne un DataFrame avec columns: code_insee, pop_2016, pop_2021, evolution_5ans.
    """
    df = df_2016.join(df_2021, how="inner", lsuffix="_2016", rsuffix="_2021")
    df = df.rename(columns={"pop_2016": "pop_2016", "pop_2021": "pop_2021"})
    df = df.reset_index()

    df["evolution_5ans"] = ((df["pop_2021"] - df["pop_2016"]) / df["pop_2016"]) * 100
    df = df.dropna(subset=["evolution_5ans"])

    print(f"\n  {len(df):,} communes avec évolution calculée")
    print(f"  Évolution médiane : {df['evolution_5ans'].median():.2f}%")
    print(f"  En croissance (>0%) : {(df['evolution_5ans'] > 0).sum():,}")
    print(f"  En déclin   (<-5%) : {(df['evolution_5ans'] < -5).sum():,}")

    return df[["code_insee", "pop_2016", "pop_2021", "evolution_5ans"]]


# ── Import principal ───────────────────────────────────────────────────────────

async def run():
    print("=== Import démographie (populations légales INSEE 2016→2021) ===\n")
    await init_db()

    # 1. Télécharger les deux fichiers
    data_2016 = await telecharger(URL_2016, "populations légales 2016 (XLS)")
    data_2021 = await telecharger(URL_2021, "populations légales 2021 (ZIP)")

    # 2. Extraire les populations
    print("\nExtraction populations 2016...")
    df_2016 = extraire_pop(data_2016, 2016, est_zip=False)

    print("\nExtraction populations 2021...")
    df_2021 = extraire_pop(data_2021, 2021, est_zip=True)

    # 3. Calcul évolution
    print("\nCalcul de l'évolution démographique 2016→2021...")
    df = calculer_evolution(df_2016, df_2021)

    # 4. Calcul du score par percentile
    serie_evo = df["evolution_5ans"]
    df["score_demographie"] = df["evolution_5ans"].apply(
        lambda x: round(percentile_to_score(x, serie_evo, "direct"), 1)
    )

    valides = df[df["score_demographie"] >= 0]
    print(f"\n  {len(valides):,} communes avec score démographie calculé")
    print(f"  Score médian : {valides['score_demographie'].median():.1f}")

    # 5. Sauvegarder en base
    print("\nMise à jour de la base de données...")
    async with async_session() as session:
        nb_ok = 0
        for _, row in valides.iterrows():
            code = row["code_insee"]
            evo = float(row["evolution_5ans"])
            score = float(row["score_demographie"])

            res = await session.execute(text("""
                UPDATE scores
                SET evolution_population_5ans = :evo,
                    score_demographie = :s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :c
            """), {"evo": evo, "s": score, "c": code})

            if res.rowcount == 0:
                await session.execute(text("""
                    INSERT OR IGNORE INTO scores
                        (code_insee, score_global, lettre,
                         score_equipements, score_securite, score_education,
                         score_sante, score_demographie, score_immobilier,
                         score_environnement, score_transports, score_revenus,
                         evolution_population_5ans, nb_categories_scorees, updated_at)
                    VALUES
                        (:c, 50, 'C', -1, -1, -1, -1, :s, -1, -1, -1, -1,
                         :evo, 1, CURRENT_TIMESTAMP)
                """), {"c": code, "s": score, "evo": evo})

            nb_ok += 1
            if nb_ok % 5000 == 0:
                await session.commit()
                print(f"  → {nb_ok:,} communes traitées...")

        await session.commit()
        print(f"  → {nb_ok:,} communes mises à jour")

    # 6. Recalcul scores globaux
    print("\nRecalcul scores globaux...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_education, score_sante, score_revenus,
                   score_transports, score_demographie, score_environnement
            FROM scores
            WHERE score_equipements >= 0 OR score_securite >= 0
               OR score_immobilier >= 0 OR score_education >= 0
               OR score_sante >= 0 OR score_revenus >= 0
               OR score_transports >= 0 OR score_demographie >= 0
               OR score_environnement >= 0
        """))
        rows = result.fetchall()
        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_education", "score_sante", "score_revenus",
                "score_transports", "score_demographie", "score_environnement"]
        cat_map = {
            "score_equipements": "equipements", "score_securite": "securite",
            "score_immobilier": "immobilier", "score_education": "education",
            "score_sante": "sante", "score_revenus": "revenus",
            "score_transports": "transports", "score_demographie": "demographie",
            "score_environnement": "environnement",
        }
        nb_recalc = 0
        for row in rows:
            r = dict(zip(cols, row))
            sous_scores = {
                cat: r[col] for col, cat in cat_map.items()
                if r[col] is not None and r[col] >= 0
            }
            if not sous_scores:
                continue
            score, lettre, nb = calculer_score_global(sous_scores)
            await session.execute(text("""
                UPDATE scores SET score_global = :sg, lettre = :l,
                    nb_categories_scorees = :nb, updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :c
            """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
            nb_recalc += 1
            if nb_recalc % 5000 == 0:
                await session.commit()
        await session.commit()
        print(f"  → {nb_recalc:,} scores globaux recalculés")

    print("\n=== Import démographie terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
