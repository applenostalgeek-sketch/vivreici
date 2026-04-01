"""
Import données éducation — IPS collèges + DNB (brevet) + lycées professionnels
Sources : data.education.gouv.fr

Méthode V2 — scoring géographique par rayon (APL-style) :
  Pour chaque commune, on agrège les établissements scolaires dans un rayon de 30km,
  pondérés par 1/distance. Toutes les communes du territoire sont scorées (pas seulement
  celles qui hébergent un établissement).

  Score final = 90% qualité + 10% proximité
    - Qualité  : percentile national du score IPS/DNB/lycée pro pondéré par distance
    - Proximité: percentile national de 1/distance_minimale (collège le plus proche)

  Composantes qualité (pondération renormalisée si source absente) :
    - IPS collèges    40% : proxy socio-économique (biais connu, maintenu)
    - DNB réussite    40% : résultats scolaires réels — contrebalance le biais IPS
    - Lycée pro       20% : taux réussite examens professionnels

Lancement :
  cd /Users/admin/vivreici && .venv/bin/python -m backend.data_import.import_education
"""

import asyncio
import math
import sys
import os

import httpx
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score, CATEGORIES


# ── Configuration ──────────────────────────────────────────────────────────────

BASE_URL   = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets"
IPS_DATASET   = "fr-en-ips-colleges-ap2023"
LYCEE_DATASET = "fr-en-indicateurs-de-resultat-des-lycees-denseignement-professionnels"
DNB_CSV_URL   = (
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets"
    "/fr-en-dnb-par-etablissement/exports/csv?limit=-1&timezone=UTC"
)

PAGE_SIZE    = 100   # maximum autorisé par l'API Opendatasoft
RAYON_KM     = 30    # rayon de recherche des établissements
MIN_DIST_KM  = 1.0   # distance minimale (évite poids infini pour même commune)


# ── Géographie ─────────────────────────────────────────────────────────────────

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance en km entre deux points GPS (formule haversine)."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ── Pagination API ──────────────────────────────────────────────────────────────

async def paginer_api(
    client: httpx.AsyncClient,
    dataset: str,
    select: str | None = None,
    where: str | None = None,
) -> list[dict]:
    url = f"{BASE_URL}/{dataset}/records"
    params: dict = {"limit": PAGE_SIZE, "offset": 0}
    if select:
        params["select"] = select
    if where:
        params["where"] = where

    resp = await client.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    total   = data.get("total_count", 0)
    records = data.get("results", [])
    print(f"  → {dataset} : {total:,} records (filtre: {where or 'aucun'})")

    MAX_OFFSET = 9900
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


# ── Téléchargement IPS collèges ────────────────────────────────────────────────

async def telecharger_ips(client: httpx.AsyncClient) -> pd.DataFrame:
    """Retourne DataFrame par commune : code_insee, ips_moyen, nb_colleges"""
    DERNIERE_RENTREE = "2024-2025"
    print(f"Téléchargement IPS collèges (rentrée {DERNIERE_RENTREE})...")
    records = await paginer_api(
        client, IPS_DATASET,
        select="code_insee_de_la_commune,ips",
        where=f'rentree_scolaire="{DERNIERE_RENTREE}"',
    )

    if not records:
        print("  AVERTISSEMENT : aucune donnée IPS reçue")
        return pd.DataFrame(columns=["code_insee", "ips_moyen", "nb_colleges"])

    df = pd.DataFrame(records).rename(columns={"code_insee_de_la_commune": "code_insee"})
    df["code_insee"] = df["code_insee"].astype(str).str.strip().str.zfill(5)
    df["ips"] = pd.to_numeric(df["ips"], errors="coerce")
    df = df.dropna(subset=["code_insee", "ips"])
    df = df[df["code_insee"].str.len() == 5]

    agg = df.groupby("code_insee").agg(
        ips_moyen=("ips", "mean"),
        nb_colleges=("ips", "count"),
    ).reset_index()

    print(f"  → {len(agg):,} communes avec données IPS collèges")
    return agg


# ── Téléchargement lycées pro ──────────────────────────────────────────────────

async def telecharger_lycees_pro(client: httpx.AsyncClient) -> pd.DataFrame:
    """Retourne DataFrame par commune : code_insee, taux_reussite_moyen, nb_lycees"""
    print("Téléchargement résultats lycées pro...")
    records = await paginer_api(
        client, LYCEE_DATASET,
        select="commune,taux_brut_de_reussite_total_secteurs"
    )

    if not records:
        print("  AVERTISSEMENT : aucune donnée lycées pro reçue")
        return pd.DataFrame(columns=["code_insee", "taux_reussite_moyen", "nb_lycees"])

    df = pd.DataFrame(records).rename(columns={"commune": "code_insee"})
    df["code_insee"] = df["code_insee"].astype(str).str.strip().str.zfill(5)
    df["taux"] = pd.to_numeric(df["taux_brut_de_reussite_total_secteurs"], errors="coerce")
    df = df.dropna(subset=["code_insee", "taux"])
    df = df[df["code_insee"].str.len() == 5]

    agg = df.groupby("code_insee").agg(
        taux_reussite_moyen=("taux", "mean"),
        nb_lycees=("taux", "count"),
    ).reset_index()

    print(f"  → {len(agg):,} communes avec données lycées pro")
    return agg


# ── Téléchargement DNB ─────────────────────────────────────────────────────────

async def telecharger_dnb(client: httpx.AsyncClient) -> pd.DataFrame:
    """Retourne DataFrame par commune : code_insee, dnb_taux_moyen, nb_colleges_dnb"""
    print("Téléchargement résultats DNB (brevet des collèges)...")
    try:
        resp = await client.get(DNB_CSV_URL, timeout=120)
        resp.raise_for_status()
        content = resp.content
        print(f"  → {len(content) // 1024:,} Ko téléchargés")
    except Exception as e:
        print(f"  AVERTISSEMENT : échec téléchargement DNB ({e}) — source ignorée")
        return pd.DataFrame(columns=["code_insee", "dnb_taux_moyen", "nb_colleges_dnb"])

    import io
    try:
        df = pd.read_csv(io.BytesIO(content), sep=";", dtype=str, on_bad_lines="skip", encoding="utf-8")
    except Exception:
        df = pd.read_csv(io.BytesIO(content), sep=";", dtype=str, on_bad_lines="skip", encoding="latin-1")

    print(f"  Colonnes : {list(df.columns[:10])}")

    col_commune = None
    for c in df.columns:
        if c.lower() == "commune":
            col_commune = c
            break
    if col_commune is None:
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


# ── Scoring géographique par rayon ─────────────────────────────────────────────

def calculer_scores_education_rayon(
    df_ips: pd.DataFrame,
    df_lycees: pd.DataFrame,
    df_dnb: pd.DataFrame,
    df_communes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calcule le score éducation pour TOUTES les communes par agrégation géographique.

    Pour chaque commune cible, cherche tous les établissements scolaires dans un
    rayon de 30km et calcule une moyenne pondérée par 1/distance.

    Retourne DataFrame : code_insee, score_education, min_dist_college_km
    """
    # ── Construire les sources (communes avec au moins un établissement) ────────
    sources = df_communes[df_communes["latitude"].notna() & df_communes["longitude"].notna()].copy()
    sources = sources.merge(df_ips[["code_insee", "ips_moyen"]], on="code_insee", how="left")
    sources = sources.merge(df_dnb[["code_insee", "dnb_taux_moyen"]], on="code_insee", how="left")
    sources = sources.merge(df_lycees[["code_insee", "taux_reussite_moyen"]], on="code_insee", how="left")

    has_data = sources[["ips_moyen", "dnb_taux_moyen", "taux_reussite_moyen"]].notna().any(axis=1)
    sources_valid = sources[has_data].copy()
    print(f"  Sources (communes avec établissements) : {len(sources_valid):,}")

    # ── Index spatial : grille 0.5° pour accélérer la recherche ───────────────
    grid: dict = {}
    for _, row in sources_valid.iterrows():
        key = (round(row["latitude"] * 2) / 2, round(row["longitude"] * 2) / 2)
        grid.setdefault(key, []).append({
            "code_insee":          row["code_insee"],
            "lat":                 row["latitude"],
            "lng":                 row["longitude"],
            "ips_moyen":           row["ips_moyen"],
            "dnb_taux_moyen":      row["dnb_taux_moyen"],
            "taux_reussite_moyen": row["taux_reussite_moyen"],
        })

    # ── Cibles : toutes communes avec coordonnées ──────────────────────────────
    targets = df_communes[df_communes["latitude"].notna() & df_communes["longitude"].notna()].copy()
    print(f"  Communes cibles : {len(targets):,}")

    results = []
    n = len(targets)
    for i, (_, target) in enumerate(targets.iterrows()):
        if i % 5000 == 0:
            print(f"    {i:,}/{n:,} communes traitées...")

        lat, lng = target["latitude"], target["longitude"]
        code     = target["code_insee"]

        # Candidats dans les 9 cellules voisines
        candidates = []
        for dlat in [-0.5, 0, 0.5]:
            for dlng in [-0.5, 0, 0.5]:
                key = (round((lat + dlat) * 2) / 2, round((lng + dlng) * 2) / 2)
                candidates.extend(grid.get(key, []))

        if not candidates:
            continue

        # Calcul des poids et agrégation
        w_ips = w_dnb = w_lycee = 0.0
        sum_ips = sum_dnb = sum_lycee = 0.0
        min_dist = float("inf")

        for src in candidates:
            d = haversine(lat, lng, src["lat"], src["lng"])
            if d > RAYON_KM:
                continue
            d_eff = max(d, MIN_DIST_KM)
            if d < min_dist:
                min_dist = d
            w = 1.0 / d_eff

            if pd.notna(src["ips_moyen"]):
                sum_ips += src["ips_moyen"] * w
                w_ips   += w
            if pd.notna(src["dnb_taux_moyen"]):
                sum_dnb += src["dnb_taux_moyen"] * w
                w_dnb   += w
            if pd.notna(src["taux_reussite_moyen"]):
                sum_lycee += src["taux_reussite_moyen"] * w
                w_lycee   += w

        results.append({
            "code_insee":          code,
            "ips_rayon":           sum_ips  / w_ips   if w_ips   > 0 else None,
            "dnb_rayon":           sum_dnb  / w_dnb   if w_dnb   > 0 else None,
            "lycee_rayon":         sum_lycee / w_lycee if w_lycee > 0 else None,
            # Clamp à MIN_DIST_KM pour éviter division par zéro (commune avec son propre collège)
            "min_dist_college_km": max(min_dist, MIN_DIST_KM) if min_dist < float("inf") else None,
        })

    df_result = pd.DataFrame(results)
    print(f"  → {len(df_result):,} communes avec données dans le rayon")

    # ── Percentiles nationaux ──────────────────────────────────────────────────
    serie_ips   = df_result["ips_rayon"].dropna()
    serie_dnb   = df_result["dnb_rayon"].dropna()
    serie_lycee = df_result["lycee_rayon"].dropna()

    # Proximité : 1/distance (plus proche = score plus élevé)
    serie_prox = (1.0 / df_result["min_dist_college_km"].dropna())

    df_result["score_ips"]   = df_result["ips_rayon"].apply(
        lambda x: percentile_to_score(x, serie_ips,   "direct") if pd.notna(x) else -1.0
    )
    df_result["score_dnb"]   = df_result["dnb_rayon"].apply(
        lambda x: percentile_to_score(x, serie_dnb,   "direct") if pd.notna(x) else -1.0
    )
    df_result["score_lycee"] = df_result["lycee_rayon"].apply(
        lambda x: percentile_to_score(x, serie_lycee, "direct") if pd.notna(x) else -1.0
    )
    df_result["score_proximite"] = df_result["min_dist_college_km"].apply(
        lambda x: percentile_to_score(1.0 / x, serie_prox, "direct") if pd.notna(x) else -1.0
    )

    # ── Score qualité : IPS 40% + DNB 40% + lycée pro 20% (renormalisé) ───────
    def composer_qualite(row) -> float:
        composantes = [
            (row["score_ips"],   0.40),
            (row["score_dnb"],   0.40),
            (row["score_lycee"], 0.20),
        ]
        valides = [(s, p) for s, p in composantes if s >= 0]
        if not valides:
            return -1.0
        poids_total = sum(p for _, p in valides)
        return sum(s * p for s, p in valides) / poids_total

    df_result["score_qualite"] = df_result.apply(composer_qualite, axis=1)

    # ── Score final : 90% qualité + 10% proximité ─────────────────────────────
    def composer_final(row) -> float:
        q = row["score_qualite"]
        p = row["score_proximite"]
        if q < 0 and p < 0:
            return -1.0
        if q < 0:
            return round(p, 1)
        if p < 0:
            return round(q, 1)
        return round(0.9 * q + 0.1 * p, 1)

    df_result["score_education"] = df_result.apply(composer_final, axis=1)

    return df_result[["code_insee", "score_education", "min_dist_college_km"]]


# ── Recalcul score global ──────────────────────────────────────────────────────

async def recalculer_scores_globaux(session) -> int:
    """Recalcule score_global et lettre pour toutes les communes scorées."""
    result = await session.execute(text("""
        SELECT code_insee,
               score_equipements, score_securite,    score_immobilier,
               score_education,   score_sante,        score_environnement,
               score_demographie, score_transports,   score_risques
        FROM scores
        WHERE score_global >= 0
           OR score_equipements >= 0 OR score_securite >= 0
           OR score_immobilier  >= 0 OR score_education >= 0
    """))
    rows = result.fetchall()
    cols = [
        "code_insee", "score_equipements", "score_securite", "score_immobilier",
        "score_education", "score_sante", "score_environnement",
        "score_demographie", "score_transports", "score_risques",
    ]
    cat_map = {
        "score_equipements":   "equipements",
        "score_securite":      "securite",
        "score_immobilier":    "immobilier",
        "score_education":     "education",
        "score_sante":         "sante",
        "score_environnement": "environnement",
        "score_demographie":   "demographie",
        "score_transports":    "transports",
        "score_risques":       "risques",
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
            SET score_global = :sg, lettre = :l,
                nb_categories_scorees = :nb, updated_at = CURRENT_TIMESTAMP
            WHERE code_insee = :c
        """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
        count += 1

    return count


# ── Point d'entrée principal ───────────────────────────────────────────────────

async def run():
    print("=== Import données éducation V2 (scoring géographique par rayon) ===\n")
    await init_db()

    # ── 1. Téléchargement des sources ─────────────────────────────────────────
    async with httpx.AsyncClient(follow_redirects=True) as client:
        df_ips    = await telecharger_ips(client)
        df_lycees = await telecharger_lycees_pro(client)
        df_dnb    = await telecharger_dnb(client)

    # ── 2. Chargement communes avec coordonnées ────────────────────────────────
    print("\nChargement communes (coordonnées GPS)...")
    async with async_session() as session:
        result = await session.execute(text(
            "SELECT code_insee, latitude, longitude FROM communes WHERE latitude IS NOT NULL"
        ))
        rows = result.fetchall()
    df_communes = pd.DataFrame(rows, columns=["code_insee", "latitude", "longitude"])
    df_communes["latitude"]  = pd.to_numeric(df_communes["latitude"],  errors="coerce")
    df_communes["longitude"] = pd.to_numeric(df_communes["longitude"], errors="coerce")
    print(f"  → {len(df_communes):,} communes avec coordonnées")

    # ── 3. Scoring géographique ────────────────────────────────────────────────
    print("\nCalcul des scores éducation par rayon géographique...")
    df = calculer_scores_education_rayon(df_ips, df_lycees, df_dnb, df_communes)

    df_scored = df[df["score_education"] >= 0]
    print(f"  → {len(df_scored):,} communes avec score éducation calculé")

    # ── 4. Upsert en base ─────────────────────────────────────────────────────
    print("\nUpsert en base (table scores)...")

    # Ajouter colonne min_dist_college_km si elle n'existe pas
    async with async_session() as session:
        try:
            await session.execute(text(
                "ALTER TABLE scores ADD COLUMN min_dist_college_km REAL DEFAULT -1"
            ))
            await session.commit()
            print("  Colonne min_dist_college_km ajoutée")
        except Exception:
            pass  # colonne déjà présente

    async with async_session() as session:
        nb_updated = nb_inserted = 0

        for _, row in df_scored.iterrows():
            code  = row["code_insee"]
            score = float(row["score_education"])
            dist  = float(row["min_dist_college_km"]) if pd.notna(row["min_dist_college_km"]) else -1.0

            result = await session.execute(
                text("UPDATE scores SET score_education=:s, min_dist_college_km=:d WHERE code_insee=:c"),
                {"s": score, "d": dist, "c": code}
            )
            if result.rowcount == 0:
                await session.execute(text("""
                    INSERT OR IGNORE INTO scores
                        (code_insee, score_global, lettre,
                         score_equipements, score_securite, score_education,
                         score_sante, score_demographie, score_immobilier,
                         score_environnement, nb_categories_scorees,
                         min_dist_college_km, updated_at)
                    VALUES
                        (:c, 50, 'C', -1, -1, :s, -1, -1, -1, -1, 1, :d, CURRENT_TIMESTAMP)
                """), {"c": code, "s": score, "d": dist})
                nb_inserted += 1
            else:
                nb_updated += 1

            if (nb_updated + nb_inserted) % 5000 == 0:
                await session.commit()
                print(f"    {nb_updated + nb_inserted:,} communes traitées...")

        await session.commit()
        print(f"  → {nb_updated:,} mises à jour, {nb_inserted:,} créées")

        # ── 5. Recalcul scores globaux ────────────────────────────────────────
        print("\nRecalcul des scores globaux (toutes catégories)...")
        nb_global = await recalculer_scores_globaux(session)
        await session.commit()
        print(f"  → {nb_global:,} communes avec score global recalculé")

    # ── 6. Résumé et contrôles ────────────────────────────────────────────────
    print("\n=== Résumé ===")
    async with async_session() as session:
        result = await session.execute(text(
            "SELECT COUNT(*) FROM scores WHERE score_education >= 0"
        ))
        total = result.scalar()
        print(f"Communes scorées en éducation : {total:,}")

        result = await session.execute(text("""
            SELECT
              SUM(CASE WHEN nb_categories_scorees = 9 THEN 1 ELSE 0 END) as c9,
              SUM(CASE WHEN nb_categories_scorees = 8 THEN 1 ELSE 0 END) as c8,
              SUM(CASE WHEN nb_categories_scorees < 8 THEN 1 ELSE 0 END) as c_moins
            FROM scores WHERE score_global >= 0
        """))
        r = result.fetchone()
        print(f"Distribution nb_categories_scorees : 9={r[0]:,}  8={r[1]:,}  <8={r[2]:,}")

        result = await session.execute(text("""
            SELECT
              AVG(min_dist_college_km) as dist_moy,
              MIN(min_dist_college_km) as dist_min,
              MAX(min_dist_college_km) as dist_max
            FROM scores WHERE min_dist_college_km > 0
        """))
        r = result.fetchone()
        print(f"Distance collège : moy={r[0]:.1f}km  min={r[1]:.1f}km  max={r[2]:.1f}km")

        result = await session.execute(text("""
            SELECT s.code_insee, c.nom, c.departement,
                   s.score_education, s.min_dist_college_km, s.score_global
            FROM scores s LEFT JOIN communes c ON c.code_insee = s.code_insee
            WHERE s.score_education >= 0
            ORDER BY s.score_education DESC LIMIT 10
        """))
        print("\nTop 10 communes score éducation :")
        print(f"  {'Code':6}  {'Commune':<28}  {'Dept':4}  {'Edu':>5}  {'Dist':>7}  {'Global':>6}")
        print("  " + "-" * 65)
        for r in result.fetchall():
            dist_str = f"{r[4]:.1f}km" if r[4] and r[4] > 0 else "local"
            print(f"  {r[0]:6}  {(r[1] or '?'):<28}  {(r[2] or '?'):4}  {r[3]:>5.1f}  {dist_str:>7}  {r[5]:>6.1f}")

        result = await session.execute(text("""
            SELECT s.code_insee, c.nom, c.departement,
                   s.score_education, s.min_dist_college_km, s.score_global
            FROM scores s LEFT JOIN communes c ON c.code_insee = s.code_insee
            WHERE s.score_education >= 0
            ORDER BY s.min_dist_college_km DESC LIMIT 10
        """))
        print("\nTop 10 communes les plus éloignées d'un collège :")
        print(f"  {'Code':6}  {'Commune':<28}  {'Dept':4}  {'Edu':>5}  {'Dist':>7}  {'Global':>6}")
        print("  " + "-" * 65)
        for r in result.fetchall():
            print(f"  {r[0]:6}  {(r[1] or '?'):<28}  {(r[2] or '?'):4}  {r[3]:>5.1f}  {r[4]:>6.1f}km  {r[5]:>6.1f}")

        # Spot-check Rambouillet
        result = await session.execute(text("""
            SELECT s.score_education, s.min_dist_college_km, s.nb_categories_scorees, s.score_global
            FROM scores s WHERE s.code_insee = '78517'
        """))
        r = result.fetchone()
        if r:
            print(f"\nSpot-check Rambouillet (78517) : edu={r[0]}  dist={r[1]}km  cats={r[2]}  global={r[3]}")


if __name__ == "__main__":
    asyncio.run(run())
