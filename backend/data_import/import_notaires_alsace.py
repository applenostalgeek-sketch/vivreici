"""
Import prix immobiliers Alsace-Moselle — API notaires (immobilier.notaires.fr)

Contexte :
  Les départements 57, 67, 68 n'ont pas de données DVF (livre foncier local).
  L'import DVF standard estime leurs prix par KNN spatial depuis les départements
  voisins → résultats incohérents (Mulhouse estimé à 3 057 €/m² vs 1 260 € réel).

Source :
  API publique immobilier.notaires.fr (pas d'authentification requise)
  Endpoint /pub-services/immodecret-stat1/v1/prix — prix médians par commune
  Données notariales réelles (transactions enregistrées)

Méthode :
  1. Récupérer les prix notaires (appartements + maisons) pour chaque commune dispo
  2. Moyenne pondérée appart/maison si les deux sont disponibles
  3. Pour les communes sans données notaires : KNN local basé sur les communes
     notaires du même département (au lieu des départements voisins)
  4. Mise à jour score_immobilier + recalcul score_global

Lancement :
  cd /Users/admin/vivreici && python3 -m backend.data_import.import_notaires_alsace
  (À lancer APRÈS import_dvf pour écraser les estimations KNN incorrectes)
"""

import asyncio
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

DEPARTEMENTS_ALSACE_MOSELLE = ["57", "67", "68"]

API_BASE = "https://immobilier.notaires.fr/pub-services/immodecret-stat1/v1"

# Période 4 = 1 an glissant (meilleur compromis volume/actualité)
PERIODE = 4

# Types de biens
TYPE_APPART = "APA"  # Appartements anciens
TYPE_MAISON = "MAA"  # Maisons anciennes

# KNN pour communes sans données notaires
K_VOISINS = 8
DIST_MAX_KM = 50  # Distance max pour le KNN (évite voisins aberrants)

EARTH_R = 6371.0


# ── Récupération API notaires ─────────────────────────────────────────────────

async def fetch_prix_departement(
    client: httpx.AsyncClient, dept: str, type_bien: str,
) -> list[dict]:
    """Récupère les prix par commune pour un département et un type de bien."""
    url = f"{API_BASE}/prix"
    params = {
        "codeInsee": dept,
        "nivGeo": "DEPARTEMENT",
        "sousNivGeo": "COMMUNE",
        "typeBien": type_bien,
        "periode": PERIODE,
    }
    resp = await client.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


async def fetch_all_notaires() -> pd.DataFrame:
    """Récupère les prix notaires pour les 3 départements, appart + maison."""
    print("Récupération des prix notaires (immobilier.notaires.fr)...")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = []

        for dept in DEPARTEMENTS_ALSACE_MOSELLE:
            for type_bien, label in [(TYPE_APPART, "appart"), (TYPE_MAISON, "maison")]:
                try:
                    data = await fetch_prix_departement(client, dept, type_bien)
                    for item in data:
                        code = item.get("inseeCommune", "")
                        prix = item.get("prixM2Median")
                        nb = item.get("nbMutation", 0)
                        if code and prix and nb and nb >= 5:
                            results.append({
                                "code_insee": str(code).zfill(5),
                                "type": label,
                                "prix_m2": float(prix),
                                "nb_mutations": int(nb),
                            })
                    print(f"  → {dept} {label}: {len([r for r in data if r.get('nbMutation',0) >= 5])} communes")
                except Exception as e:
                    print(f"  ⚠ {dept} {label}: erreur — {e}")

    df = pd.DataFrame(results)
    if df.empty:
        print("  ⚠ Aucune donnée récupérée !")
        return df

    # Pivot : une ligne par commune avec prix appart et/ou maison
    pivot = df.pivot_table(
        index="code_insee",
        columns="type",
        values=["prix_m2", "nb_mutations"],
        aggfunc="first",
    )
    pivot.columns = [f"{col[0]}_{col[1]}" for col in pivot.columns]
    pivot = pivot.reset_index()

    # Calculer prix composite (moyenne pondérée par nb transactions)
    def prix_composite(row):
        prix_a = row.get("prix_m2_appart")
        prix_m = row.get("prix_m2_maison")
        nb_a = row.get("nb_mutations_appart", 0) or 0
        nb_m = row.get("nb_mutations_maison", 0) or 0

        if pd.notna(prix_a) and pd.notna(prix_m) and nb_a > 0 and nb_m > 0:
            return (prix_a * nb_a + prix_m * nb_m) / (nb_a + nb_m)
        elif pd.notna(prix_a):
            return prix_a
        elif pd.notna(prix_m):
            return prix_m
        return None

    pivot["prix_m2_median"] = pivot.apply(prix_composite, axis=1)
    pivot["nb_transactions"] = (
        pivot.get("nb_mutations_appart", pd.Series(0)).fillna(0)
        + pivot.get("nb_mutations_maison", pd.Series(0)).fillna(0)
    ).astype(int)

    result = pivot[["code_insee", "prix_m2_median", "nb_transactions"]].dropna(subset=["prix_m2_median"])
    result["prix_m2_median"] = result["prix_m2_median"].round(2)

    print(f"\n  → {len(result)} communes avec prix notaires")
    print(f"  → Prix min/median/max : {result['prix_m2_median'].min():.0f} / "
          f"{result['prix_m2_median'].median():.0f} / {result['prix_m2_median'].max():.0f} €/m²")

    return result


# ── KNN local pour communes sans données notaires ─────────────────────────────

async def knn_local(session, df_notaires: pd.DataFrame) -> pd.DataFrame:
    """
    Estime les prix des communes Alsace-Moselle sans données notaires
    en utilisant les communes notaires comme points de référence.
    KNN pondéré par 1/d², limité à DIST_MAX_KM km.
    """
    print(f"\n── KNN local (ancres = communes notaires, max {DIST_MAX_KM} km) ──")

    from scipy.spatial import cKDTree

    # Charger GPS de toutes les communes 57/67/68
    placeholders = ",".join(f"'{d}%'" for d in DEPARTEMENTS_ALSACE_MOSELLE)
    result = await session.execute(
        text(f"""
            SELECT c.code_insee, c.latitude, c.longitude, c.population
            FROM communes c
            WHERE ({" OR ".join(f"c.code_insee LIKE '{d}%'" for d in DEPARTEMENTS_ALSACE_MOSELLE)})
            AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
        """)
    )
    all_communes = {r[0]: {"lat": r[1], "lng": r[2], "pop": r[3] or 0} for r in result.fetchall()}
    print(f"  → {len(all_communes)} communes Alsace-Moselle avec GPS")

    # Points de référence = communes avec données notaires
    notaires_codes = set(df_notaires["code_insee"])
    notaires_prix = dict(zip(df_notaires["code_insee"], df_notaires["prix_m2_median"]))

    # Construire le KDTree avec les communes notaires
    ref_codes = []
    ref_coords = []
    ref_prix = []
    for code, prix in notaires_prix.items():
        if code in all_communes:
            c = all_communes[code]
            ref_codes.append(code)
            ref_coords.append([np.radians(c["lat"]) * EARTH_R, np.radians(c["lng"]) * EARTH_R])
            ref_prix.append(prix)

    if not ref_coords:
        print("  ⚠ Pas de points de référence pour le KNN !")
        return df_notaires

    tree = cKDTree(np.array(ref_coords))
    ref_prix = np.array(ref_prix)
    print(f"  → {len(ref_codes)} points de référence notaires dans le KDTree")

    # Communes sans données notaires
    communes_sans = [c for c in all_communes if c not in notaires_codes]
    print(f"  → {len(communes_sans)} communes à estimer")

    rows_knn = []
    nb_too_far = 0

    for code in communes_sans:
        c = all_communes[code]
        coord = np.array([np.radians(c["lat"]) * EARTH_R, np.radians(c["lng"]) * EARTH_R])

        k = min(K_VOISINS, len(ref_codes))
        dists, idxs = tree.query(coord, k=k)

        if np.isscalar(dists):
            dists = [dists]
            idxs = [idxs]

        weights = []
        prix_v = []
        for d, idx in zip(dists, idxs):
            if idx >= len(ref_prix):
                continue
            dist_km = d  # déjà en km (coords * EARTH_R)
            if dist_km > DIST_MAX_KM:
                continue
            w = 1.0 / max(dist_km, 0.5) ** 2
            weights.append(w)
            prix_v.append(ref_prix[idx])

        if not weights:
            nb_too_far += 1
            continue

        prix_estime = np.average(prix_v, weights=np.array(weights))
        rows_knn.append({
            "code_insee": code,
            "prix_m2_median": round(float(prix_estime), 2),
            "nb_transactions": 0,
        })

    print(f"  → {len(rows_knn)} communes estimées par KNN local")
    if nb_too_far:
        print(f"  → {nb_too_far} communes trop éloignées (> {DIST_MAX_KM} km)")

    # Combiner notaires + KNN
    df_notaires = df_notaires.copy()
    df_notaires["est_estime"] = False
    if rows_knn:
        df_knn = pd.DataFrame(rows_knn)
        df_knn["est_estime"] = True
        df_combined = pd.concat([df_notaires, df_knn], ignore_index=True)
    else:
        df_combined = df_notaires

    return df_combined


# ── Mise à jour en base ──────────────────────────────────────────────────────

async def update_db(session, df: pd.DataFrame):
    """Met à jour les prix et scores immobilier pour les communes Alsace-Moselle."""
    print("\nMise à jour de la base de données...")

    # 1. Charger la distribution nationale complète pour recalculer les percentiles
    result = await session.execute(
        text("SELECT code_insee, prix_m2_median FROM scores WHERE prix_m2_median > 0")
    )
    all_prix = {r[0]: r[1] for r in result.fetchall()}

    # Remplacer les prix Alsace-Moselle par les nouvelles valeurs
    for _, row in df.iterrows():
        all_prix[row["code_insee"]] = row["prix_m2_median"]

    # Recalculer le percentile pour TOUTES les communes (la distribution change)
    prix_series = pd.Series(list(all_prix.values()))

    # Score immobilier = percentile inversé
    def score_immo(prix):
        return round((1 - (prix_series < prix).mean()) * 100, 1)

    # 2. Mettre à jour les communes Alsace-Moselle
    count = 0
    for _, row in df.iterrows():
        code = row["code_insee"]
        prix = row["prix_m2_median"]
        estime = 1 if row.get("est_estime", False) else 0
        score = score_immo(prix)

        await session.execute(
            text("""
                UPDATE scores
                SET prix_m2_median = :prix,
                    prix_m2_estime = :estime,
                    score_immobilier = :score,
                    updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :code
            """),
            {"prix": prix, "estime": estime, "score": score, "code": code},
        )
        count += 1

    await session.commit()
    print(f"  → {count} communes Alsace-Moselle mises à jour")

    # 3. Recalculer score_global pour ces communes
    print("Recalcul scores globaux Alsace-Moselle...")
    codes = tuple(df["code_insee"].tolist())
    cats = list(CATEGORIES.keys())
    cat_cols = ", ".join(f"score_{cat}" for cat in cats)

    result = await session.execute(
        text(f"""
            SELECT code_insee, {cat_cols} FROM scores
            WHERE {" OR ".join(f"code_insee LIKE '{d}%'" for d in DEPARTEMENTS_ALSACE_MOSELLE)}
        """)
    )

    nb_recalc = 0
    for row in result.fetchall():
        code_insee = row[0]
        sous_scores = {}
        for j, cat in enumerate(cats):
            val = row[j + 1]
            if val is not None and val >= 0:
                sous_scores[cat] = float(val)

        score, lettre, nb = calculer_score_global(sous_scores)
        await session.execute(
            text("""
                UPDATE scores
                SET score_global = :sg, lettre = :l, nb_categories_scorees = :nb,
                    updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :code
            """),
            {"sg": score, "l": lettre, "nb": nb, "code": code_insee},
        )
        nb_recalc += 1

    await session.commit()
    print(f"  → {nb_recalc} scores globaux recalculés")


# ── Vérification ─────────────────────────────────────────────────────────────

async def verifier(session):
    """Affiche les résultats pour vérification."""
    villes = [
        ("67482", "Strasbourg"), ("68224", "Mulhouse"), ("68066", "Colmar"),
        ("57463", "Metz"), ("57672", "Thionville"), ("67180", "Haguenau"),
        ("57631", "Sarreguemines"), ("68297", "Saint-Louis"),
    ]

    print("\n── Vérification villes principales ──")
    print(f"  {'Ville':<22} {'Prix m²':>8} {'Estimé':>7} {'Score':>6} {'Global':>7} {'Lettre':>7}")
    print(f"  {'-'*60}")

    for code, nom in villes:
        r = await session.execute(
            text("""
                SELECT prix_m2_median, prix_m2_estime, score_immobilier, score_global, lettre
                FROM scores WHERE code_insee = :code
            """),
            {"code": code},
        )
        row = r.fetchone()
        if row:
            est = "KNN" if row[1] == 1 else "réel"
            print(f"  {nom:<22} {row[0]:>7.0f}€ {est:>7} {row[2]:>5.1f} {row[3]:>6.1f} {row[4]:>7}")

    # Stats par département
    print(f"\n── Stats par département ──")
    for dept in DEPARTEMENTS_ALSACE_MOSELLE:
        r = await session.execute(
            text("""
                SELECT COUNT(*),
                       ROUND(AVG(prix_m2_median)),
                       ROUND(AVG(score_immobilier), 1),
                       SUM(CASE WHEN prix_m2_estime = 1 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN prix_m2_estime = 0 THEN 1 ELSE 0 END)
                FROM scores
                WHERE code_insee LIKE :dept AND prix_m2_median > 0
            """),
            {"dept": dept + "%"},
        )
        row = r.fetchone()
        print(f"  {dept}: {row[0]} communes, prix moyen {row[1]}€/m², "
              f"score moyen {row[2]}, notaires={row[4]}, KNN={row[3]}")


# ── Point d'entrée ─────────────────────────────────────────────────────────────

async def run():
    print("=== Import prix notaires Alsace-Moselle (67/68/57) ===\n")
    await init_db()

    # 1. Récupérer les données notaires
    df_notaires = await fetch_all_notaires()
    if df_notaires.empty:
        print("Abandon — pas de données.")
        return

    # 2. KNN local pour les communes sans données
    async with async_session() as session:
        df_combined = await knn_local(session, df_notaires)

    nb_notaires = len(df_notaires)
    nb_total = len(df_combined)
    print(f"\n  → {nb_notaires} communes notaires + {nb_total - nb_notaires} KNN = {nb_total} total")

    # 3. Mise à jour en base
    async with async_session() as session:
        await update_db(session, df_combined)
        await verifier(session)

    print(f"\n=== Import notaires terminé — {nb_total} communes mises à jour ===")


if __name__ == "__main__":
    asyncio.run(run())
