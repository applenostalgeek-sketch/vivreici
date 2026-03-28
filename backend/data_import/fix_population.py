"""
Correctif : populations manquantes (= 0) dans la table communes.

2028 communes ont population=0 en DB — leur score_equipements est donc 0
(nb_equip / population = division par zéro → score 0/100).

Source : populations légales 2021 INSEE (même source que import_demographie.py).

Actions :
1. Télécharge les populations légales 2021
2. Met à jour communes.population pour les communes à 0
3. Recalcule nb_equipements/1000hab pour toutes les communes
4. Recalcule score_equipements (percentile national — doit être recalibré globalement)
5. Recalcule score_global pour les communes affectées

Usage : python3 -m backend.data_import.fix_population
"""

import asyncio
import io
import os
import sys
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import httpx
import numpy as np
import pandas as pd
from sqlalchemy import text

from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score

URL_2021 = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"


async def telecharger_pop2021() -> dict:
    """Retourne {code_insee: population} depuis les populations légales 2021 INSEE."""
    print("Téléchargement populations légales 2021...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=120) as client:
        resp = await client.get(URL_2021)
        resp.raise_for_status()
    print(f"  → {len(resp.content) // 1024:,} Ko")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        noms = zf.namelist()
        cible = next(
            (n for n in noms if "commune" in n.lower() and n.endswith(".csv")),
            next((n for n in noms if "commune" in n.lower()), noms[0]),
        )
        print(f"  → Fichier : {cible}")
        data = zf.read(cible)

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

    cols_lower = {str(c).lower().strip(): str(c) for c in df.columns}

    def find(*kw):
        for k in kw:
            for cl, co in cols_lower.items():
                if k in cl:
                    return co
        return None

    col_geo  = find("codgeo")
    col_dept = find("code département", "codedep", "code_dep", "dep")
    col_com  = find("codcom", "code commune", "code_com", "codcom", "com")
    col_pop  = find("population municipale", "pmunicip", "pmun", "psdc", "ptot")

    if col_pop is None:
        raise ValueError(f"Colonne population introuvable. Colonnes : {list(df.columns)}")

    if col_geo:
        df["code_insee"] = df[col_geo].astype(str).str.strip().str.zfill(5)
    elif col_com:
        # COM peut être soit 5 chars (code complet) soit 3 chars (code local)
        sample = df[col_com].dropna().iloc[0]
        if len(str(sample).strip()) >= 5:
            df["code_insee"] = df[col_com].astype(str).str.strip().str.zfill(5)
        elif col_dept:
            df["code_insee"] = (df[col_dept].astype(str).str.strip().str.zfill(2) +
                                df[col_com].astype(str).str.strip().str.zfill(3))
        else:
            raise ValueError(f"Impossible de construire le code INSEE. Colonnes : {list(df.columns)}")
    else:
        raise ValueError(f"Code commune introuvable. Colonnes : {list(df.columns)}")

    df["pop"] = pd.to_numeric(df[col_pop], errors="coerce")
    df = df[df["code_insee"].str.match(r"^\d{5}$") & (df["pop"] > 0)].dropna(subset=["pop"])
    df["pop"] = df["pop"].astype(int)
    df = df.drop_duplicates(subset="code_insee")

    result = dict(zip(df["code_insee"], df["pop"]))
    print(f"  → {len(result):,} communes avec population")
    return result


async def run():
    print("=== Correctif populations manquantes ===\n")
    await init_db()

    pop2021 = await telecharger_pop2021()

    async with async_session() as session:
        # ── 1. Identifier les communes avec pop=0 ───────────────────────────
        result = await session.execute(text("""
            SELECT code_insee FROM communes WHERE population = 0 OR population IS NULL
        """))
        codes_zero = [r[0] for r in result.fetchall()]
        print(f"\nCommunes avec population=0 : {len(codes_zero)}")

        fixed = 0
        not_found = []
        for code in codes_zero:
            pop = pop2021.get(code)
            if pop:
                await session.execute(text(
                    "UPDATE communes SET population = :p WHERE code_insee = :c"
                ), {"p": pop, "c": code})
                fixed += 1
            else:
                not_found.append(code)

        await session.commit()
        print(f"  → {fixed} populations corrigées")
        if not_found:
            print(f"  → {len(not_found)} codes non trouvés dans INSEE 2021 (communes fusionnées ?)")

        # ── 2. Charger toutes les communes pour recalibrer le percentile ────
        result = await session.execute(text("""
            SELECT c.code_insee, c.population, s.nb_equipements
            FROM communes c JOIN scores s ON s.code_insee = c.code_insee
            WHERE c.population > 0 AND s.nb_equipements >= 0
        """))
        rows = result.fetchall()

    codes   = [r[0] for r in rows]
    pops    = np.array([r[1] for r in rows], dtype=float)
    nb_equip = np.array([r[2] for r in rows], dtype=float)

    # equip pour 1000 hab
    equip_pour_1000 = np.where(pops > 0, nb_equip / pops * 1000, 0.0)
    serie = pd.Series(equip_pour_1000)

    scores_eq = np.array([
        percentile_to_score(v, serie, "direct") for v in equip_pour_1000
    ])

    print(f"\nRecalcul score_equipements ({len(codes):,} communes)...")
    print(f"  Score médian : {np.median(scores_eq[scores_eq >= 0]):.1f}")

    # Communes dont le score change (pop était 0 avant → score_eq était 0)
    nb_changed = sum(1 for c in codes if c in set(codes_zero))
    print(f"  Communes avec score corrigé : {nb_changed}")

    # ── 3. Sauvegarder les scores équipements ───────────────────────────────
    async with async_session() as session:
        for i in range(0, len(codes), 5000):
            for j in range(min(5000, len(codes) - i)):
                idx = i + j
                await session.execute(text("""
                    UPDATE scores SET score_equipements = :s WHERE code_insee = :c
                """), {"s": round(float(scores_eq[idx]), 4), "c": codes[idx]})
            await session.commit()
            print(f"  → {min(i+5000, len(codes)):,}/{len(codes):,}")

        # ── 4. Recalcul scores globaux ─────────────────────────────────────
        print("\nRecalcul scores globaux...")
        result = await session.execute(text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_education, score_sante, score_transports,
                   score_environnement, score_demographie
            FROM scores
        """))
        all_rows = result.fetchall()

        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_education", "score_sante", "score_transports",
                "score_environnement", "score_demographie"]
        cat_map = {
            "score_equipements": "equipements", "score_securite": "securite",
            "score_immobilier":  "immobilier",  "score_education": "education",
            "score_sante":       "sante",        "score_transports": "transports",
            "score_environnement": "environnement", "score_demographie": "demographie",
        }
        nb_recalc = 0
        for row in all_rows:
            r = dict(zip(cols, row))
            sous_scores = {cat: r[col] for col, cat in cat_map.items()
                           if r[col] is not None and r[col] >= 0}
            if not sous_scores:
                continue
            score, lettre, nb = calculer_score_global(sous_scores)
            await session.execute(text("""
                UPDATE scores SET score_global=:sg, lettre=:l,
                    nb_categories_scorees=:nb, updated_at=CURRENT_TIMESTAMP
                WHERE code_insee=:c
            """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
            nb_recalc += 1
            if nb_recalc % 5000 == 0:
                await session.commit()
        await session.commit()
        print(f"  → {nb_recalc:,} scores globaux recalculés")

    # ── 5. Validation ───────────────────────────────────────────────────────
    import sqlite3, pathlib
    conn = sqlite3.connect(str(pathlib.Path(__file__).parent.parent.parent / "vivreici.db"))
    print("\nValidation post-correctif :")

    villes = [("87085","Limoges"), ("95018","Argenteuil"), ("50129","Cherbourg-en-Cotentin"),
              ("73065","Chambéry"), ("11069","Carcassonne"), ("95127","Cergy")]
    for code, nom in villes:
        r = conn.execute("""
            SELECT c.population, s.score_equipements, s.score_global, s.lettre
            FROM communes c JOIN scores s ON s.code_insee=c.code_insee
            WHERE c.code_insee=?
        """, (code,)).fetchone()
        if r:
            print(f"  {nom:<25} pop={r[0]:,} eq={r[1]:.0f} → {r[3]}({r[2]:.1f})")

    conn.close()
    print("\n=== Correctif terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
