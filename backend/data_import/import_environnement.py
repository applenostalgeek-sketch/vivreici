"""
Import taux d'artificialisation des sols par commune.

Source : Observatoire de l'artificialisation des sols (CEREMA / DGALN)
         Données publiées sur data.developpement-durable.gouv.fr
         Fichier : Consommation d'espaces NAF (naturels, agricoles, forestiers) par commune

Métrique : taux_espaces_nat = 100 - taux_artificialisation
           (% de la surface communale occupée par espaces naturels/agricoles/forestiers)
Score    : percentile national direct (plus d'espaces naturels = meilleur environnement)

Méthode alternative : si la source principale est inaccessible, utilise les données
IGN/INSEE sur la surface par type d'occupation du sol (fichiers fonciers, etc.)

Lancement :
  cd /Users/admin/vivreici && .venv/bin/python -m backend.data_import.import_environnement
"""

import asyncio
import sys
import os
import io
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
import pandas as pd
import numpy as np

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score


# ── Sources de données ────────────────────────────────────────────────────────

# Source principale — data.gouv.fr, Observatoire de l'artificialisation des sols
# Dataset : "Artificialisation des sols — Données par commune"
# Colonnes : idcom (5 chiffres), naf09art23 (ha consommés 2009-2023), artcom23 (ha artificialisés 2023), supcom (ha totale)
URL_ARTIF_COMMUNE = (
    "https://static.data.gouv.fr/resources/artificialisation-des-collectivites-de-france"
    "/20260129-121544/artif-commune.csv"
)


# ── Téléchargement avec fallback ──────────────────────────────────────────────

async def telecharger_artif() -> pd.DataFrame | None:
    """
    Télécharge le CSV d'artificialisation par commune depuis data.gouv.fr.
    Retourne un DataFrame {code_insee, taux_espaces_nat} ou None si échec.
    """
    print(f"Téléchargement données artificialisation (data.gouv.fr)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=180) as client:
        try:
            resp = await client.get(URL_ARTIF_COMMUNE)
            resp.raise_for_status()
            print(f"  → {len(resp.content) // 1024:,} Ko reçus")
        except Exception as e:
            print(f"  → Erreur téléchargement : {e}")
            return None

    df = parser_artif(resp.content, "csv")
    if df is not None and len(df) > 1000:
        print(f"  → {len(df):,} communes parsées")
        return df
    print(f"  → résultat insuffisant ({len(df) if df is not None else 0} communes)")
    return None


def parser_artif(content: bytes, fmt: str = "csv") -> pd.DataFrame | None:
    """Parse le contenu CSV brut."""
    for sep in [",", ";"]:
        for enc in ["utf-8", "latin-1"]:
            try:
                df = pd.read_csv(io.BytesIO(content), sep=sep, dtype=str,
                                 on_bad_lines="skip", encoding=enc)
                if len(df.columns) >= 4:
                    print(f"  Colonnes (sep={sep!r}) : {list(df.columns[:8])}")
                    result = extraire_taux_artif(df)
                    if result is not None and len(result) > 100:
                        return result
            except Exception:
                continue
    return None


def extraire_taux_artif(df: pd.DataFrame, fmt: str = "csv") -> pd.DataFrame | None:
    """
    Extrait taux_espaces_nat depuis le fichier data.gouv.fr artif-commune.csv.
    Colonnes : commune_code, pourcent_artif_2 (taux artificialisation millesime le plus récent).
    """
    cols_lower = [c.lower() for c in df.columns]
    cols_orig  = list(df.columns)

    def find_col(*candidates):
        for c in candidates:
            for i, col_l in enumerate(cols_lower):
                if c in col_l:
                    return cols_orig[i]
        return None

    # Code commune
    col_code = find_col("commune_code", "idcom", "codgeo", "code_insee", "insee")
    if col_code is None:
        print(f"  Colonne code introuvable. Colonnes : {cols_orig[:8]}")
        return None

    # Taux d'artificialisation — prendre le millesime le plus récent (_2)
    col_taux = find_col("pourcent_artif_2", "pourcent_artif", "tauxartif", "taux_artif")
    if col_taux is None:
        print(f"  Colonne taux introuvable. Colonnes : {cols_orig[:8]}")
        return None

    print(f"  Colonnes : code={col_code}, taux={col_taux}")

    result = pd.DataFrame()
    result["code_insee"] = df[col_code].astype(str).str.strip().str.zfill(5)
    result["taux_artif"] = pd.to_numeric(df[col_taux], errors="coerce")

    result = result.dropna(subset=["taux_artif"])
    result = result[result["code_insee"].str.match(r"^\d{5}$")]
    result["taux_espaces_nat"] = 100 - result["taux_artif"].clip(0, 100)
    result = result.drop_duplicates(subset="code_insee", keep="last")
    return result[["code_insee", "taux_espaces_nat"]]


# ── Import principal ───────────────────────────────────────────────────────────

async def run():
    print("=== Import environnement (taux d'artificialisation des sols) ===\n")
    await init_db()

    # 1. Télécharger et parser
    df = await telecharger_artif()

    if df is None or len(df) < 100:
        print("\nERREUR : aucune source disponible pour les données d'artificialisation.")
        print("Le score_environnement reste à -1 (non calculé).")
        return

    # 2. Score par percentile inverse (moins d'artificialisation = meilleur)
    serie = df["taux_espaces_nat"]
    df["score_environnement"] = df["taux_espaces_nat"].apply(
        lambda x: round(percentile_to_score(x, serie, "direct"), 1)
    )

    valides = df[df["score_environnement"] >= 0]
    print(f"\n  {len(valides):,} communes avec score environnement calculé")
    print(f"  Taux espaces nat. médian : {valides['taux_espaces_nat'].median():.1f}%")
    print(f"  Score médian : {valides['score_environnement'].median():.1f}")

    # 3. Sauvegarder en base
    print("\nMise à jour de la base de données...")
    async with async_session() as session:
        nb_ok = 0
        for _, row in valides.iterrows():
            code = row["code_insee"]
            score = float(row["score_environnement"])

            res = await session.execute(text("""
                UPDATE scores
                SET score_environnement = :s, updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :c
            """), {"s": score, "c": code})

            if res.rowcount == 0:
                await session.execute(text("""
                    INSERT OR IGNORE INTO scores
                        (code_insee, score_global, lettre,
                         score_equipements, score_securite, score_education,
                         score_sante, score_demographie, score_immobilier,
                         score_environnement, score_transports, score_revenus,
                         nb_categories_scorees, updated_at)
                    VALUES
                        (:c, 50, 'C', -1, -1, -1, -1, -1, -1, :s, -1, -1,
                         1, CURRENT_TIMESTAMP)
                """), {"c": code, "s": score})

            nb_ok += 1
            if nb_ok % 5000 == 0:
                await session.commit()
                print(f"  → {nb_ok:,} communes traitées...")

        await session.commit()
        print(f"  → {nb_ok:,} communes mises à jour")

    # 4. Recalcul scores globaux
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

    print("\n=== Import environnement terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
