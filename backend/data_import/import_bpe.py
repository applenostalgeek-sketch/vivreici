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

# Codes d'équipements sélectionnés (nomenclature BPE 2024)
# Source : https://www.insee.fr/fr/information/8221498
# ATTENTION : les codes ont été entièrement refondus entre BPE 2023 et BPE 2024.
EQUIPEMENTS_SELECTIONNES = {
    # Commerces alimentaires
    "B105": "supermarché",        # ex-B101
    "B104": "hypermarché",        # ex-B102
    "B207": "boulangerie",        # ex-B201
    "B204": "boucherie",          # ex-B203

    # Santé
    "D265": "médecin_généraliste",  # ex-D101
    "D307": "pharmacie",            # ex-D231 (ATTENTION : D307 = urgences en BPE 2023)
    "D101": "hôpital",              # ex-D303 (ATTENTION : D101 = médecin gén. en BPE 2023)
    "D106": "urgences",             # ex-D307

    # Services publics
    "A129": "mairie",             # ex-A101
    "A206": "bureau_poste",       # ex-A116
    "A203": "agence_bancaire",    # ex-A304

    # Éducation
    "C107": "école_maternelle",   # ex-C101
    "C109": "école_élémentaire",  # ex-C104
    "C201": "collège",            # inchangé
    "C301": "lycée",              # inchangé
    "C302": "lycée_professionnel",  # inchangé

    # Sports
    "F121": "gymnase",            # ex-F101
    "F113": "terrain_football",   # ex-F102 (terrains de grands jeux)
    "F101": "piscine",            # ex-F111 (ATTENTION : F101 = gymnase en BPE 2023)
    "F120": "salle_sport",        # ex-F302

    # Culture et loisirs
    "F303": "cinéma",             # ex-F201 (ATTENTION : F303 = bibliothèque en BPE 2023)
    "F307": "bibliothèque",       # ex-F303
    "F315": "théâtre",            # ex-F310 (arts du spectacle)

    # Transports
    "E107": "gare_nationale",     # ex-H102 (gares grandes lignes)
    "E108": "gare_régionale",     # ex-H102 (gares TER)
    "E109": "gare_locale",        # ex-H102 (gares de proximité)
}

# Codes médecins pour calcul densité médicale (BPE 2024)
CODES_MEDECINS = {"D265"}  # médecin_généraliste uniquement (spécialistes = D251-D276, trop éclatés)
CODES_PHARMACIES = {"D307"}
CODES_SERVICES_PUBLICS = {"A129", "A206", "A203"}
CODES_SPORTS_LOISIRS = {"F121", "F113", "F101", "F120", "F303", "F307", "F315"}
CODES_TRANSPORTS = {"E107", "E108", "E109"}
CODES_ALIMENTAIRE = {"B105", "B104", "B207", "B204"}

# Score de présence : poids si le type est PRÉSENT dans la commune.
# Règle : min(count, 1) × poids — avoir 50 pharmacies vaut autant qu'en avoir 1.
# Pas de division par population → aucun biais taille de commune.
# 0 = exclu (doublon avec un autre score, présent partout, ou peu discriminant)
POIDS_PRESENCE = {
    # Alimentation quotidienne
    "B105": 4,   # supermarché
    "B104": 4,   # hypermarché (distinct du supermarché — avoir les deux est un bonus légitime)
    "B207": 2,   # boulangerie
    "B204": 1,   # boucherie

    # Santé — médecins exclus (→ score_sante APL)
    "D265": 0,   # médecin_généraliste → score_sante
    "D307": 4,   # pharmacie — essentielle
    "D101": 2,   # hôpital (soins courte durée)
    "D106": 2,   # urgences

    # Services publics
    "A129": 0,   # mairie — présente partout, ne différencie pas
    "A206": 2,   # bureau de poste
    "A203": 0,   # agence bancaire — données BPE peu fiables

    # Éducation — exclu (score_education séparé)
    "C107": 0, "C109": 0, "C201": 0, "C301": 0, "C302": 0,

    # Sports — inclus à poids faible
    "F121": 1,   # gymnase
    "F113": 0,   # terrain_football — trop répandu, ne discrimine pas
    "F101": 1,   # piscine
    "F120": 0,   # salle_sport (regroupé avec gymnase dans la pratique)

    # Culture
    "F303": 1,   # cinéma
    "F307": 1,   # bibliothèque / médiathèque
    "F315": 1,   # théâtre / arts du spectacle

    # Transports — exclu (→ score_transports séparé)
    "E107": 0,   # gare nationale
    "E108": 0,   # gare régionale
    "E109": 0,   # gare locale
}
# Score max théorique : 4+4+2+1+4+2+2+2+1+1+1+1+1 = 26 pts (commune avec tout)

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

    # Score de présence : min(count, 1) × poids pour chaque type
    # Avoir 50 pharmacies vaut autant qu'en avoir 1 — on mesure la PRÉSENCE, pas la densité
    cols_presence = [(code, poids) for code, poids in POIDS_PRESENCE.items()
                     if poids > 0 and code in pivot.columns]
    pivot["presence_score"] = sum(
        pivot[code].clip(upper=1) * poids for code, poids in cols_presence
    )

    # Comptage pondéré (count × poids, sans cap) — utilisé pour la densité dans le score hybride
    pivot["weighted_count"] = sum(
        pivot[code] * poids for code, poids in cols_presence
    )

    # Construire le JSON de détail par type (seulement les types présents)
    label_map = {k: v for k, v in EQUIPEMENTS_SELECTIONNES.items() if k in pivot.columns}
    def build_detail(row):
        d = {label_map[code]: int(row[code]) for code in label_map if int(row.get(code, 0)) > 0}
        return json.dumps(d, ensure_ascii=False) if d else None
    pivot["equipements_detail"] = pivot.apply(build_detail, axis=1)

    return pivot[["code_insee", "nb_equipements_total", "presence_score", "weighted_count",
                  "nb_essentiels", "nb_medecins", "nb_pharmacies", "nb_sports_loisirs",
                  "nb_transports", "nb_alimentaire", "equipements_detail"]]


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
    int_cols = ["nb_equipements_total", "presence_score", "weighted_count", "nb_essentiels",
                "nb_medecins", "nb_pharmacies", "nb_sports_loisirs", "nb_transports", "nb_alimentaire"]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    if "equipements_detail" in df.columns:
        df["equipements_detail"] = df["equipements_detail"].where(df["equipements_detail"].notna(), None)

    # Métriques normalisées
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
