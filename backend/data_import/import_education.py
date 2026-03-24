"""
Import données éducation — IPS collèges + DNB (brevet) + lycées professionnels
Sources : data.education.gouv.fr

Datasets utilisés :
  - fr-en-ips-colleges-ap2023 : IPS (Indice de Position Sociale) des collèges depuis 2023
    Colonnes clés : code_insee_de_la_commune, ips
  - fr-en-dnb-par-etablissement : résultats DNB (brevet des collèges) par établissement
    Export CSV ; colonnes clés : commune (code INSEE), taux_de_reussite ("78,30%")
  - fr-en-indicateurs-de-resultat-des-lycees-denseignement-professionnels :
    Taux de réussite lycées pro
    Colonnes clés : commune (code INSEE 5 chiffres), taux_brut_de_reussite_total_secteurs

Méthode de scoring :
  - IPS collèges 40% : proxy niveau socio-économique (biais connu, maintenu pour info)
  - DNB taux réussite 40% : mesure directe des résultats académiques — réduit le biais IPS
  - Lycée pro taux réussite 20% : formation professionnelle
  - Score final = percentile pondéré des sources disponibles

Lancement :
  cd /Users/admin/vivreici && .venv/bin/python -m backend.data_import.import_education
"""

import asyncio
import sys
import os

import httpx
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score, CATEGORIES


# ── Configuration API ──────────────────────────────────────────────────────────

BASE_URL = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets"

IPS_DATASET    = "fr-en-ips-colleges-ap2023"
LYCEE_DATASET  = "fr-en-indicateurs-de-resultat-des-lycees-denseignement-professionnels"
DNB_CSV_URL    = (
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets"
    "/fr-en-dnb-par-etablissement/exports/csv?limit=-1&timezone=UTC"
)

PAGE_SIZE = 100  # maximum autorisé par l'API


# ── Pagination API ─────────────────────────────────────────────────────────────

async def paginer_api(
    client: httpx.AsyncClient,
    dataset: str,
    select: str | None = None,
    where: str | None = None,
) -> list[dict]:
    """
    Pagine un dataset data.education.gouv.fr jusqu'à tout récupérer.
    L'API Opendatasoft refuse les offsets >= 10 000 : on s'arrête à 9 900.
    Utiliser le paramètre `where` pour filtrer et rester sous cette limite.
    Retourne la liste complète des records.
    """
    url = f"{BASE_URL}/{dataset}/records"
    params: dict = {"limit": PAGE_SIZE, "offset": 0}
    if select:
        params["select"] = select
    if where:
        params["where"] = where

    # Premier appel pour connaître le total
    resp = await client.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    total = data.get("total_count", 0)
    records = data.get("results", [])
    print(f"  → {dataset} : {total:,} records (filtre: {where or 'aucun'})")

    MAX_OFFSET = 9900  # limite dure de l'API Opendatasoft
    while len(records) < min(total, MAX_OFFSET + PAGE_SIZE):
        next_offset = len(records)
        if next_offset > MAX_OFFSET:
            print(f"  AVERTISSEMENT : limite API atteinte à {MAX_OFFSET} — arrêt pagination")
            break
        params["offset"] = next_offset
        resp = await client.get(url, params=params, timeout=60)
        resp.raise_for_status()
        batch = resp.json().get("results", [])
        if not batch:
            break
        records.extend(batch)
        if len(records) % 2000 == 0:
            print(f"    {len(records):,}/{total:,} téléchargés...")

    print(f"  → {len(records):,} records récupérés pour {dataset}")
    return records


# ── Téléchargement et agrégation IPS collèges ─────────────────────────────────

async def telecharger_ips(client: httpx.AsyncClient) -> pd.DataFrame:
    """
    Télécharge les IPS collèges et retourne un DataFrame agrégé par commune.
    Colonnes résultantes : code_insee, ips_moyen, nb_colleges
    """
    # L'API refuse les offsets >= 10 000 → filtrer directement sur la rentrée la plus récente
    DERNIERE_RENTREE = "2024-2025"
    print(f"Téléchargement IPS collèges (fr-en-ips-colleges-ap2023, rentrée {DERNIERE_RENTREE})...")
    records = await paginer_api(
        client, IPS_DATASET,
        select="code_insee_de_la_commune,ips",
        where=f'rentree_scolaire="{DERNIERE_RENTREE}"',
    )

    if not records:
        print("  AVERTISSEMENT : aucune donnée IPS reçue")
        return pd.DataFrame(columns=["code_insee", "ips_moyen", "nb_colleges"])

    df = pd.DataFrame(records)
    df = df.rename(columns={"code_insee_de_la_commune": "code_insee"})

    # Nettoyage
    df["code_insee"] = df["code_insee"].astype(str).str.strip().str.zfill(5)
    df["ips"] = pd.to_numeric(df["ips"], errors="coerce")
    df = df.dropna(subset=["code_insee", "ips"])
    df = df[df["code_insee"].str.len() == 5]

    # Agrégation par commune : IPS moyen
    agg = df.groupby("code_insee").agg(
        ips_moyen=("ips", "mean"),
        nb_colleges=("ips", "count"),
    ).reset_index()

    print(f"  → {len(agg):,} communes avec données IPS collèges")
    return agg


# ── Téléchargement et agrégation résultats lycées pro ─────────────────────────

async def telecharger_lycees_pro(client: httpx.AsyncClient) -> pd.DataFrame:
    """
    Télécharge les résultats lycées pro et retourne un DataFrame agrégé par commune.
    Colonnes résultantes : code_insee, taux_reussite_moyen, nb_lycees
    """
    print("Téléchargement résultats lycées pro...")
    records = await paginer_api(
        client, LYCEE_DATASET,
        select="commune,taux_brut_de_reussite_total_secteurs"
    )

    if not records:
        print("  AVERTISSEMENT : aucune donnée lycées pro reçue")
        return pd.DataFrame(columns=["code_insee", "taux_reussite_moyen", "nb_lycees"])

    df = pd.DataFrame(records)
    df = df.rename(columns={"commune": "code_insee"})

    # Nettoyage
    df["code_insee"] = df["code_insee"].astype(str).str.strip().str.zfill(5)
    df["taux_reussite_moyen_raw"] = pd.to_numeric(
        df["taux_brut_de_reussite_total_secteurs"], errors="coerce"
    )
    df = df.dropna(subset=["code_insee", "taux_reussite_moyen_raw"])
    df = df[df["code_insee"].str.len() == 5]

    # Agrégation par commune
    agg = df.groupby("code_insee").agg(
        taux_reussite_moyen=("taux_reussite_moyen_raw", "mean"),
        nb_lycees=("taux_reussite_moyen_raw", "count"),
    ).reset_index()

    print(f"  → {len(agg):,} communes avec données lycées pro")
    return agg


# ── Téléchargement DNB (brevet des collèges) ──────────────────────────────────

async def telecharger_dnb(client: httpx.AsyncClient) -> pd.DataFrame:
    """
    Télécharge les résultats DNB (brevet des collèges) par établissement.
    Source : export CSV national, délimiteur ";"
    Retourne un DataFrame agrégé par commune : code_insee, dnb_taux_moyen, nb_colleges_dnb
    """
    print(f"Téléchargement résultats DNB (brevet des collèges)...")
    try:
        resp = await client.get(DNB_CSV_URL, timeout=120)
        resp.raise_for_status()
        content = resp.content
        print(f"  → {len(content) // 1024:,} Ko téléchargés")
    except Exception as e:
        print(f"  AVERTISSEMENT : échec téléchargement DNB ({e}) — source ignorée")
        return pd.DataFrame(columns=["code_insee", "dnb_taux_moyen", "nb_colleges_dnb"])

    # Le fichier est délimité par ";" (format data.education.gouv.fr)
    import io
    try:
        df = pd.read_csv(
            io.BytesIO(content),
            sep=";",
            dtype=str,
            on_bad_lines="skip",
            encoding="utf-8",
        )
    except Exception:
        df = pd.read_csv(
            io.BytesIO(content),
            sep=";",
            dtype=str,
            on_bad_lines="skip",
            encoding="latin-1",
        )

    print(f"  Colonnes : {list(df.columns[:10])}")

    # Chercher les colonnes code commune et taux réussite
    col_commune = None
    for c in df.columns:
        if c.lower() in ["commune", "code_commune", "cod_commune", "numero_uai_etablissement"]:
            pass
        if c.lower() == "commune":
            col_commune = c
            break
    if col_commune is None:
        # Chercher une colonne qui ressemble à des codes INSEE
        for c in df.columns:
            sample = df[c].dropna().head(50)
            valids = sample.apply(lambda x: str(x).strip().zfill(5).isdigit() and len(str(x).strip()) <= 6).mean()
            if valids > 0.7:
                col_commune = c
                break

    col_taux = None
    for c in df.columns:
        if "taux" in c.lower() and "reussite" in c.lower():
            col_taux = c
            break

    if col_commune is None or col_taux is None:
        print(f"  AVERTISSEMENT : colonnes introuvables (commune={col_commune}, taux={col_taux}) — DNB ignoré")
        return pd.DataFrame(columns=["code_insee", "dnb_taux_moyen", "nb_colleges_dnb"])

    print(f"  Colonnes identifiées : commune={col_commune}, taux={col_taux}")

    df_clean = pd.DataFrame()
    df_clean["code_insee"] = df[col_commune].astype(str).str.strip().str.zfill(5)
    # Taux peut être "78,30%" ou "78.30" ou "78"
    df_clean["taux"] = (
        df[col_taux].astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )
    df_clean["taux"] = pd.to_numeric(df_clean["taux"], errors="coerce")

    df_clean = df_clean.dropna(subset=["taux"])
    df_clean = df_clean[df_clean["code_insee"].str.match(r"^\d{5}$")]
    df_clean = df_clean[df_clean["taux"].between(0, 100)]

    # Filtrer sur l'année la plus récente si colonne année présente
    col_annee = None
    for c in df.columns:
        if "annee" in c.lower() or "session" in c.lower():
            col_annee = c
            break
    if col_annee is not None:
        annees_valides = df[col_annee].dropna().unique()
        if len(annees_valides) > 0:
            annee_max = max(str(a) for a in annees_valides)
            mask = df[col_annee].astype(str) == annee_max
            df_clean = df_clean[mask.values[:len(df_clean)]]
            print(f"  Filtre année : {annee_max}")

    agg = df_clean.groupby("code_insee").agg(
        dnb_taux_moyen=("taux", "mean"),
        nb_colleges_dnb=("taux", "count"),
    ).reset_index()

    print(f"  → {len(agg):,} communes avec données DNB brevet")
    return agg


# ── Calcul des scores ─────────────────────────────────────────────────────────

def calculer_scores_education(
    df_ips: pd.DataFrame,
    df_lycees: pd.DataFrame,
    df_dnb: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calcule le score éducation par commune (0-100, percentile national).

    Méthode (pondération renormalisée selon données disponibles) :
      - IPS collèges    40% : proxy socio-économique (biais connu, maintenu)
      - DNB réussite    40% : résultats scolaires réels — contrebalance le biais IPS
      - Lycée pro       20% : taux réussite examens professionnels
    """
    # Fusionner les trois sources
    df = df_ips.merge(df_lycees, on="code_insee", how="outer")
    df = df.merge(df_dnb, on="code_insee", how="outer")

    # Score IPS (percentile direct)
    serie_ips = df["ips_moyen"].dropna()
    df["score_ips"] = df["ips_moyen"].apply(
        lambda x: percentile_to_score(x, serie_ips, "direct") if pd.notna(x) else -1.0
    )

    # Score DNB (percentile direct)
    serie_dnb = df["dnb_taux_moyen"].dropna()
    df["score_dnb"] = df["dnb_taux_moyen"].apply(
        lambda x: percentile_to_score(x, serie_dnb, "direct") if pd.notna(x) else -1.0
    )

    # Score taux réussite lycées pro (percentile direct)
    serie_reussite = df["taux_reussite_moyen"].dropna()
    df["score_lycee"] = df["taux_reussite_moyen"].apply(
        lambda x: percentile_to_score(x, serie_reussite, "direct") if pd.notna(x) else -1.0
    )

    # Score éducation composite — poids renormalisés si source absente
    def composer_score(row):
        composantes = [
            (row["score_ips"],   0.40),
            (row["score_dnb"],   0.40),
            (row["score_lycee"], 0.20),
        ]
        scores_valides = [(s, p) for s, p in composantes if s >= 0]
        if not scores_valides:
            return -1.0
        poids_total = sum(p for _, p in scores_valides)
        return round(sum(s * p for s, p in scores_valides) / poids_total, 1)

    df["score_education"] = df.apply(composer_score, axis=1)

    return df[["code_insee", "score_education", "ips_moyen", "nb_colleges",
               "dnb_taux_moyen", "nb_colleges_dnb",
               "taux_reussite_moyen", "nb_lycees"]]


# ── Recalcul score global ──────────────────────────────────────────────────────

async def recalculer_scores_globaux(session) -> int:
    """
    Recalcule score_global et lettre pour toutes les communes qui ont
    au moins un sous-score >= 0. Retourne le nombre de communes mises à jour.
    """
    result = await session.execute(text("""
        SELECT code_insee,
               score_equipements, score_securite, score_immobilier,
               score_education,   score_sante,     score_revenus,
               score_environnement, score_demographie
        FROM scores
        WHERE score_equipements >= 0
           OR score_securite    >= 0
           OR score_immobilier  >= 0
           OR score_education   >= 0
           OR score_sante       >= 0
           OR score_revenus     >= 0
           OR score_environnement >= 0
           OR score_demographie >= 0
    """))
    rows = result.fetchall()
    cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
            "score_education", "score_sante", "score_revenus",
            "score_environnement", "score_demographie"]
    # Correspondance col → clé CATEGORIES
    cat_map = {
        "score_equipements":  "equipements",
        "score_securite":     "securite",
        "score_immobilier":   "immobilier",
        "score_education":    "education",
        "score_sante":        "sante",
        "score_revenus":      "revenus",
        "score_environnement":"environnement",
        "score_demographie":  "demographie",
    }

    count = 0
    for row in rows:
        r = dict(zip(cols, row))
        sous_scores = {
            cat: r[col]
            for col, cat in cat_map.items()
            if r[col] is not None and r[col] >= 0
        }
        if not sous_scores:
            continue
        score, lettre, nb = calculer_score_global(sous_scores)
        await session.execute(text("""
            UPDATE scores
            SET score_global = :sg,
                lettre = :l,
                nb_categories_scorees = :nb,
                updated_at = CURRENT_TIMESTAMP
            WHERE code_insee = :c
        """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
        count += 1

    return count


# ── Point d'entrée principal ───────────────────────────────────────────────────

async def run():
    print("=== Import données éducation ===\n")
    await init_db()

    async with httpx.AsyncClient(follow_redirects=True) as client:
        df_ips    = await telecharger_ips(client)
        df_lycees = await telecharger_lycees_pro(client)
        df_dnb    = await telecharger_dnb(client)

    print("\nCalcul des scores éducation par commune...")
    df = calculer_scores_education(df_ips, df_lycees, df_dnb)

    df_scored = df[df["score_education"] >= 0]
    print(f"  → {len(df_scored):,} communes avec un score éducation calculé")

    print("\nUpsert en base (table scores)...")
    async with async_session() as session:
        nb_updated = 0
        nb_inserted = 0

        for _, row in df_scored.iterrows():
            code = row["code_insee"]
            score = float(row["score_education"])

            # Tenter UPDATE d'abord
            result = await session.execute(
                text("UPDATE scores SET score_education=:s WHERE code_insee=:c"),
                {"s": score, "c": code}
            )
            if result.rowcount == 0:
                # La ligne n'existe pas encore → INSERT minimal
                await session.execute(text("""
                    INSERT OR IGNORE INTO scores
                        (code_insee, score_global, lettre,
                         score_equipements, score_securite, score_education,
                         score_sante, score_demographie, score_immobilier,
                         score_environnement, nb_categories_scorees, updated_at)
                    VALUES
                        (:c, 50, 'C', -1, -1, :s, -1, -1, -1, -1, 1, CURRENT_TIMESTAMP)
                """), {"c": code, "s": score})
                nb_inserted += 1
            else:
                nb_updated += 1

            if (nb_updated + nb_inserted) % 2000 == 0:
                await session.commit()
                print(f"    {nb_updated + nb_inserted:,} communes traitées...")

        await session.commit()
        print(f"  → {nb_updated:,} lignes mises à jour, {nb_inserted:,} lignes créées")

        # Recalcul scores globaux
        print("\nRecalcul des scores globaux...")
        nb_global = await recalculer_scores_globaux(session)
        await session.commit()
        print(f"  → {nb_global:,} communes avec score global recalculé")

    # ── Résumé final ──────────────────────────────────────────────────────────
    print("\n=== Résumé ===")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT COUNT(*) FROM scores WHERE score_education >= 0
        """))
        total = result.scalar()
        print(f"Communes scorées en éducation : {total:,}")

        result = await session.execute(text("""
            SELECT s.code_insee, c.nom, c.departement, s.score_education, s.score_global
            FROM scores s
            LEFT JOIN communes c ON c.code_insee = s.code_insee
            WHERE s.score_education >= 0
            ORDER BY s.score_education DESC
            LIMIT 10
        """))
        top = result.fetchall()
        print("\nTop 10 communes par score éducation :")
        print(f"  {'Code':6}  {'Commune':<30}  {'Dept':5}  {'Score édu':>10}  {'Score global':>12}")
        print("  " + "-" * 70)
        for r in top:
            nom = r[1] or "?"
            dept = r[2] or "?"
            print(f"  {r[0]:6}  {nom:<30}  {dept:5}  {r[3]:>10.1f}  {r[4]:>12.1f}")


if __name__ == "__main__":
    asyncio.run(run())
