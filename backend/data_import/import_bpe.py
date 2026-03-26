"""
Import des équipements et services depuis la BPE (Base Permanente des Équipements) — INSEE.
Source : https://www.insee.fr/fr/statistiques/3568629
Téléchargement automatique du fichier CSV.

Équipements pertinents pour le score :
- Commerces alimentaires (supermarchés, boulangeries, etc.)
- Santé (médecins généralistes, pharmacies, hôpitaux)
- Services publics (mairie, poste, banque)
- Sports et loisirs (gymnases, piscines, cinémas)
- Transports (gares, arrêts bus/tram structurants)
"""

import asyncio
import httpx
import pandas as pd
import io
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_scores_batch, normaliser_par_habitant, calculer_score_global


# URL du fichier BPE 2024 (ensemble des équipements)
BPE_URL = "https://www.insee.fr/fr/statistiques/fichier/8217525/BPE24.zip"

# Codes d'équipements sélectionnés (nomenclature BPE)
# Source : https://www.insee.fr/fr/metadonnees/source/fichier/bpe_nomenclatures_2023.pdf
EQUIPEMENTS_SELECTIONNES = {
    # Commerces alimentaires
    "B101": "supermarché",
    "B102": "hypermarché",
    "B201": "boulangerie",
    "B203": "boucherie",

    # Santé
    "D101": "médecin_généraliste",
    "D102": "médecin_spécialiste",
    "D231": "pharmacie",
    "D303": "hôpital",
    "D307": "urgences",

    # Services publics
    "A101": "mairie",
    "A116": "bureau_poste",
    "A304": "agence_bancaire",

    # Éducation
    "C101": "école_maternelle",
    "C104": "école_élémentaire",
    "C201": "collège",
    "C301": "lycée",
    "C302": "lycée_professionnel",

    # Sports
    "F101": "gymnase",
    "F102": "terrain_football",
    "F111": "piscine",
    "F302": "salle_sport",

    # Culture et loisirs
    "F201": "cinéma",
    "F303": "bibliothèque",
    "F310": "théâtre",

    # Transports
    "H102": "gare_voyageurs",
}

# Codes médecins pour calcul densité médicale
CODES_MEDECINS = {"D101", "D102"}
CODES_PHARMACIES = {"D231"}
CODES_SERVICES_PUBLICS = {"A101", "A116", "A304"}
CODES_SPORTS_LOISIRS = {"F101", "F102", "F111", "F302", "F201", "F303", "F310"}
CODES_TRANSPORTS = {"H102"}
CODES_ALIMENTAIRE = {"B101", "B102", "B201", "B203"}

# Poids par type d'équipement pour le score pondéré
# 0 = exclu (déjà dans un autre score, présent partout, ou mal référencé)
POIDS_EQUIPEMENTS = {
    # Alimentaire — accès quotidien, très différenciant
    "B101": 4,   # supermarché
    "B102": 5,   # hypermarché
    "B201": 2,   # boulangerie
    "B203": 1,   # boucherie

    # Santé — médecins exclus (→ score_sante séparé via APL)
    "D101": 0,   # médecin_généraliste → score_sante
    "D102": 0,   # médecin_spécialiste → score_sante
    "D231": 5,   # pharmacie — essentielle
    "D303": 3,   # hôpital
    "D307": 3,   # urgences

    # Services publics
    "A101": 0,   # mairie — présente dans toutes les communes, ne différencie pas
    "A116": 2,   # bureau_poste
    "A304": 0,   # agence_bancaire — exclu (mal référencé dans la BPE)

    # Éducation — exclu du score (biais petites communes + qualité = score_education séparé)
    "C101": 0,   # école_maternelle
    "C104": 0,   # école_élémentaire
    "C201": 0,   # collège
    "C301": 0,   # lycée
    "C302": 0,   # lycée_professionnel

    # Sports — exclus du score (biais petites communes : 1 piscine × 5pts/hab >> grandes villes)
    "F101": 0,   # gymnase
    "F102": 0,   # terrain_football
    "F111": 0,   # piscine
    "F302": 0,   # salle_sport

    # Culture — exclus du score (même biais)
    "F201": 0,   # cinéma
    "F303": 0,   # bibliothèque
    "F310": 0,   # théâtre

    # Transports — exclu (→ score_transports séparé)
    "H102": 0,   # gare_voyageurs
}

# Services du quotidien — pour rétrocompatibilité display
CODES_ESSENTIELS = CODES_ALIMENTAIRE | CODES_PHARMACIES | CODES_SERVICES_PUBLICS


async def telecharger_bpe() -> pd.DataFrame:
    """Télécharge et parse le fichier BPE depuis l'INSEE."""
    print("Téléchargement du fichier BPE (peut prendre quelques minutes)...")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(BPE_URL, timeout=300)
        resp.raise_for_status()

    print(f"  → Fichier reçu ({len(resp.content) / 1024 / 1024:.1f} Mo). Décompression...")

    import zipfile
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        # Trouver le fichier CSV dans l'archive
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            raise ValueError("Aucun fichier CSV trouvé dans l'archive BPE")

        csv_name = csv_files[0]
        print(f"  → Lecture de {csv_name}...")

        with z.open(csv_name) as f:
            df = pd.read_csv(f, sep=";", dtype={"DEPCOM": str, "TYPEQU": str}, low_memory=False)

    print(f"  → {len(df):,} équipements chargés")
    return df


def aggreger_par_commune(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège les équipements par commune et type."""
    # Filtrer sur les équipements qui nous intéressent
    codes_voulus = set(EQUIPEMENTS_SELECTIONNES.keys())
    df_filtre = df[df["TYPEQU"].isin(codes_voulus)].copy()

    print(f"  → {len(df_filtre):,} équipements retenus (sur {len(codes_voulus)} types)")

    # Compter par commune et type
    pivot = df_filtre.groupby(["DEPCOM", "TYPEQU"]).size().unstack(fill_value=0)
    pivot.columns.name = None
    pivot = pivot.reset_index().rename(columns={"DEPCOM": "code_insee"})

    # Calculer les métriques agrégées
    pivot["nb_equipements_total"] = pivot[
        [c for c in pivot.columns if c != "code_insee"]
    ].sum(axis=1)

    pivot["nb_medecins"] = pivot[[c for c in CODES_MEDECINS if c in pivot.columns]].sum(axis=1)
    pivot["nb_pharmacies"] = pivot[[c for c in CODES_PHARMACIES if c in pivot.columns]].sum(axis=1)
    pivot["nb_sports_loisirs"] = pivot[[c for c in CODES_SPORTS_LOISIRS if c in pivot.columns]].sum(axis=1)
    pivot["nb_transports"] = pivot[[c for c in CODES_TRANSPORTS if c in pivot.columns]].sum(axis=1)
    pivot["nb_alimentaire"] = pivot[[c for c in CODES_ALIMENTAIRE if c in pivot.columns]].sum(axis=1)
    # Essentiels = alimentation + pharmacie + services publics (rétrocompatibilité)
    pivot["nb_essentiels"] = pivot[[c for c in CODES_ESSENTIELS if c in pivot.columns]].sum(axis=1)

    # Score pondéré : chaque type d'équipement contribue selon son importance réelle
    # Médecins, mairie, agence_bancaire, gare = poids 0 (autres scores ou non-différenciant)
    cols_poids = [(code, poids) for code, poids in POIDS_EQUIPEMENTS.items()
                  if poids > 0 and code in pivot.columns]
    pivot["nb_equipements_pondere"] = sum(
        pivot[code] * poids for code, poids in cols_poids
    )

    # Construire le JSON de détail par type (seulement les types présents)
    label_map = {k: v for k, v in EQUIPEMENTS_SELECTIONNES.items() if k in pivot.columns}
    def build_detail(row):
        d = {label_map[code]: int(row[code]) for code in label_map if int(row.get(code, 0)) > 0}
        return json.dumps(d, ensure_ascii=False) if d else None
    pivot["equipements_detail"] = pivot.apply(build_detail, axis=1)

    return pivot[["code_insee", "nb_equipements_total", "nb_equipements_pondere", "nb_essentiels",
                  "nb_medecins", "nb_pharmacies", "nb_sports_loisirs", "nb_transports",
                  "nb_alimentaire", "equipements_detail"]]


async def run():
    """Import et scoring des équipements BPE."""
    await init_db()

    # Télécharger le fichier BPE
    df_bpe = await telecharger_bpe()

    # Agréger par commune
    print("Agrégation des équipements par commune...")
    df_equip = aggreger_par_commune(df_bpe)

    # Charger TOUTES les communes depuis la base (pas seulement celles avec équipements)
    print("Chargement des communes...")
    async with async_session() as session:
        result = await session.execute(
            text("SELECT code_insee, population FROM communes")
        )
        df_all = pd.DataFrame(result.fetchall(), columns=["code_insee", "population"])

    # LEFT JOIN communes → équipements (les communes sans équipements obtiennent 0)
    df = df_all.merge(df_equip, on="code_insee", how="left")
    df["population"] = df["population"].fillna(0).astype(int)
    # Remplir les colonnes équipements à 0 pour les communes sans données BPE
    int_cols = ["nb_equipements_total", "nb_equipements_pondere", "nb_essentiels", "nb_medecins",
                "nb_pharmacies", "nb_sports_loisirs", "nb_transports", "nb_alimentaire"]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    if "equipements_detail" in df.columns:
        df["equipements_detail"] = df["equipements_detail"].where(df["equipements_detail"].notna(), None)

    # Calculer les métriques normalisées
    # Score basé sur le count pondéré (médecins/mairie/gare exclus — autres scores)
    # Poids = importance réelle : pharmacie×5, supermarché×4, cinéma×3, boulangerie×2...
    df["equipements_pour_1000"] = df.apply(
        lambda r: normaliser_par_habitant(r["nb_equipements_pondere"], r["population"], 1000), axis=1
    )
    df["medecins_pour_10000"] = df.apply(
        lambda r: normaliser_par_habitant(r["nb_medecins"], r["population"], 10000), axis=1
    )

    # Calculer les scores
    print("Calcul des scores par commune...")
    df = calculer_scores_batch(df)

    # Upsert en base — insertion ciblée qui préserve les scores des autres modules
    print("Sauvegarde en base...")
    async with async_session() as session:
        count = 0
        for _, row in df.iterrows():
            await session.execute(text("""
                INSERT INTO scores (
                    code_insee, score_global, lettre,
                    score_equipements, score_sante,
                    score_securite, score_immobilier, score_education,
                    score_environnement, score_demographie,
                    nb_equipements, nb_medecins_pour_10000, nb_gares, nb_categories_scorees,
                    taux_criminalite, prix_m2_median, evolution_population_5ans,
                    equipements_detail, updated_at
                ) VALUES (
                    :code, :sg, :lettre, :seq, -1, -1, -1, -1, -1, -1,
                    :nb_eq, 0, :nb_gares, :nb_cat, 0, 0, 0,
                    :detail, CURRENT_TIMESTAMP
                )
                ON CONFLICT(code_insee) DO UPDATE SET
                    score_equipements       = excluded.score_equipements,
                    nb_equipements          = excluded.nb_equipements,
                    equipements_detail      = excluded.equipements_detail,
                    updated_at              = excluded.updated_at
            """), {
                "code":    row["code_insee"],
                "sg":      float(row.get("score_global", 50)),
                "lettre":  row.get("lettre", "C"),
                "seq":     float(row.get("score_equipements", -1)),
                "nb_eq":   int(row.get("nb_equipements_total", 0)),
                "nb_gares": int(row.get("nb_transports", 0)),
                "nb_cat":  int(row.get("nb_categories", 1)),
                "detail":  row.get("equipements_detail") or None,
            })
            count += 1

            if count % 1000 == 0:
                await session.commit()
                print(f"  → {count}/{len(df)} communes scorées")

        await session.commit()

    print(f"{count} communes scorées avec les données BPE.")

    # Recalcul des scores globaux en tenant compte de tous les sous-scores disponibles
    print("Recalcul des scores globaux...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_education,   score_sante,     score_environnement,
                   score_demographie, score_revenus,   score_transports
            FROM scores
            WHERE score_equipements >= 0
               OR score_securite    >= 0
               OR score_immobilier  >= 0
               OR score_education   >= 0
               OR score_sante       >= 0
               OR score_revenus     >= 0
               OR score_transports  >= 0
               OR score_environnement >= 0
               OR score_demographie >= 0
        """))
        rows = result.fetchall()
        cols = ["code_insee", "score_equipements", "score_securite", "score_immobilier",
                "score_education", "score_sante", "score_environnement",
                "score_demographie", "score_revenus", "score_transports"]
        cat_map = {
            "score_equipements":   "equipements",
            "score_securite":      "securite",
            "score_immobilier":    "immobilier",
            "score_education":     "education",
            "score_sante":         "sante",
            "score_environnement": "environnement",
            "score_demographie":   "demographie",
            "score_revenus":       "revenus",
            "score_transports":    "transports",
        }
        nb_recalc = 0
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
                SET score_global = :sg, lettre = :l, nb_categories_scorees = :nb,
                    updated_at = CURRENT_TIMESTAMP
                WHERE code_insee = :c
            """), {"sg": score, "l": lettre, "nb": nb, "c": r["code_insee"]})
            nb_recalc += 1
            if nb_recalc % 5000 == 0:
                await session.commit()
        await session.commit()
    print(f"  → {nb_recalc} scores globaux recalculés.")


if __name__ == "__main__":
    asyncio.run(run())
