"""
Import risques naturels — GASPAR bulk ZIP (Géorisques)

Source : https://files.georisques.fr/GASPAR/gaspar.zip
Fichier utilisé : risq_gaspar.csv — 172 595 lignes, 32 523 communes exposées

Risques naturels retenus (5 catégories, indicateurs binaires par commune) :
  - Inondation (toutes formes : crue, ruissellement, submersion, remontées nappes) → poids 0.35
  - Séisme                                                                         → poids 0.30
  - Mouvement de terrain (glissement, éboulement, affaissement, tassement, RGA…)  → poids 0.20
  - Feu de forêt                                                                   → poids 0.10
  - Avalanche                                                                       → poids 0.05

Méthode : indice composite 0-1 par commune → percentile national → score inversé 0-100
          (100 = aucun risque réglementé, 0 = tous les risques présents)

Note : les risques technologiques (TMD, industrie, nucléaire, radon) et météorologiques
       (tempête, grêle, foudre) sont exclus — ils ne relèvent pas de la qualité du lieu de vie.
"""

import asyncio
import httpx
import zipfile
import io
import csv
import pandas as pd
import sys
import os
import pathlib
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import percentile_to_score, calculer_score_global


GASPAR_ZIP_URL = "https://files.georisques.fr/GASPAR/gaspar.zip"
GASPAR_CSV     = "risq_gaspar.csv"  # fichier à lire dans le ZIP

# Poids du risque composite (somme = 1.0)
RISK_WEIGHTS = {
    'inondation':  0.35,
    'seisme':      0.30,
    'mvt_terrain': 0.20,
    'foret':       0.10,
    'avalanche':   0.05,
}

# Classification des libellés GASPAR → catégories (minuscules, partial match)
# Source : risq_gaspar.csv — lib_risque exhaustif issu de l'analyse du fichier réel
RISK_PATTERNS = {
    'inondation': [
        'inondation', 'crue', 'ruissellement', 'remontée', 'submersion', 'lave torrentielle',
    ],
    'seisme': [
        'séisme',
    ],
    'mvt_terrain': [
        'mouvement de terrain', 'glissement', 'éboulement', 'eboulement', 'affaissement',
        'effondrement', 'tassement', 'érosion', 'erosion', 'recul du trait', 'avancée dunaire',
        'coulée', 'écoulement', 'retrait', 'cavité',
    ],
    'foret': [
        'feu de forêt', 'feu de foret', 'incendie de forêt', 'incendie de foret',
    ],
    'avalanche': [
        'avalanche',
    ],
}


def classify_risk(libelle: str) -> str | None:
    """Classifie un libellé GASPAR → catégorie de risque naturel. None si hors-scope."""
    lb = libelle.lower()
    for risk, patterns in RISK_PATTERNS.items():
        if any(p in lb for p in patterns):
            return risk
    return None


async def telecharger_zip() -> bytes:
    """Télécharge le ZIP GASPAR (~6 Mo)."""
    print(f"Téléchargement GASPAR ZIP...")
    async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
        resp = await client.get(GASPAR_ZIP_URL)
        resp.raise_for_status()
    taille = len(resp.content) / 1024 / 1024
    print(f"  → {taille:.1f} Mo reçus")
    return resp.content


def parser_gaspar(zip_content: bytes) -> dict[str, set]:
    """
    Parse risq_gaspar.csv depuis le ZIP.
    Retourne : dict cod_commune → set des catégories de risques naturels présents
    """
    risks_by_commune: dict[str, set] = {}
    nb_unknown = 0

    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        with z.open(GASPAR_CSV) as f:
            content = f.read().decode('utf-8', errors='replace')

    reader = csv.DictReader(io.StringIO(content), delimiter=';')
    rows = list(reader)
    print(f"  → {len(rows):,} lignes GASPAR, parsing...")

    for row in rows:
        code = row.get('cod_commune', '').strip()
        libelle = row.get('lib_risque', '').strip()
        if not code:
            continue
        risk_type = classify_risk(libelle)
        if risk_type:
            risks_by_commune.setdefault(code, set()).add(risk_type)
        else:
            nb_unknown += 1

    print(f"  → {len(risks_by_commune):,} communes avec au moins 1 risque naturel")
    if nb_unknown:
        print(f"  → {nb_unknown:,} libellés non retenus (risques technos/météo, hors scope)")
    return risks_by_commune


def calculer_risque_composite(risk_types: set) -> float:
    """Indice de risque 0-1 (plus élevé = plus risqué)."""
    return sum(RISK_WEIGHTS[r] for r in RISK_WEIGHTS if r in risk_types)


async def migrer_colonnes():
    """Ajoute score_risques et risques_detail si absents (migration sans alembic)."""
    async with async_session() as session:
        for table in ['scores', 'iris_scores']:
            try:
                await session.execute(text(
                    f"ALTER TABLE {table} ADD COLUMN score_risques REAL DEFAULT -1"
                ))
                await session.commit()
                print(f"Colonne score_risques ajoutée à {table}")
            except Exception:
                pass  # déjà présente
        try:
            await session.execute(text(
                "ALTER TABLE scores ADD COLUMN risques_detail TEXT"
            ))
            await session.commit()
            print("Colonne risques_detail ajoutée à scores")
        except Exception:
            pass  # déjà présente


async def run():
    """Import complet : téléchargement → parsing → score → upsert DB."""
    await init_db()
    await migrer_colonnes()

    # 1. Téléchargement + parsing
    zip_content = await telecharger_zip()
    risks_by_commune = parser_gaspar(zip_content)
    del zip_content  # libérer la mémoire

    # Stats par catégorie
    print("\nCoverage par catégorie de risque naturel :")
    for risk, poids in RISK_WEIGHTS.items():
        n = sum(1 for risks in risks_by_commune.values() if risk in risks)
        print(f"  {risk:<15}: {n:,} communes  (poids {poids:.0%})")

    # 2. Charger tous les scores existants en base
    print("\nChargement des scores existants...")
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT code_insee,
                   score_equipements, score_securite, score_immobilier,
                   score_demographie, score_education, score_sante,
                   score_environnement, score_transports
            FROM scores
        """))
        rows = result.fetchall()

    cols = ['code_insee', 'score_equipements', 'score_securite', 'score_immobilier',
            'score_demographie', 'score_education', 'score_sante',
            'score_environnement', 'score_transports']
    df = pd.DataFrame(rows, columns=cols)
    print(f"  {len(df):,} communes en base")

    # 3. Calcul indice composite + détail des catégories présentes
    df['risque_composite'] = df['code_insee'].apply(
        lambda code: calculer_risque_composite(risks_by_commune.get(code, set()))
    )
    df['risques_detail'] = df['code_insee'].apply(
        lambda code: ','.join(sorted(risks_by_commune[code])) if code in risks_by_commune else None
    )
    n_zero = (df['risque_composite'] == 0).sum()
    print(f"\nDistribution :")
    print(f"  Sans risque PPR : {n_zero:,} ({100*n_zero/len(df):.0f}%)")
    print(f"  Avec risque PPR : {len(df)-n_zero:,} ({100*(len(df)-n_zero)/len(df):.0f}%)")
    print(f"  Max (tous risques) : {df['risque_composite'].max():.2f}")

    # 4. Score percentile inversé
    serie = df['risque_composite']
    df['score_risques'] = df['risque_composite'].apply(
        lambda x: percentile_to_score(x, serie, "inverse")
    )
    sr = df['score_risques']
    print(f"  Score risques → min={sr.min():.1f}  médiane={sr.median():.1f}  max={sr.max():.1f}")

    # 5. Upsert + recalcul score_global
    print(f"\nMise à jour de {len(df):,} communes...")
    async with async_session() as session:
        count = 0
        for _, row in df.iterrows():
            code = row['code_insee']
            new_risk = float(row['score_risques'])

            def _safe(v):
                return float(v) if v is not None and not pd.isna(v) and v >= 0 else -1.0

            sous_scores = {
                'equipements':   _safe(row['score_equipements']),
                'securite':      _safe(row['score_securite']),
                'immobilier':    _safe(row['score_immobilier']),
                'demographie':   _safe(row['score_demographie']),
                'education':     _safe(row['score_education']),
                'sante':         _safe(row['score_sante']),
                'environnement': _safe(row['score_environnement']),
                'transports':    _safe(row['score_transports']),
                'risques':       new_risk,
            }
            score_global, lettre, nb_cat = calculer_score_global(sous_scores)

            await session.execute(text("""
                UPDATE scores
                SET score_risques         = :sr,
                    risques_detail        = :rd,
                    score_global          = :sg,
                    lettre                = :l,
                    nb_categories_scorees = :nb,
                    updated_at            = :now
                WHERE code_insee = :code
            """), {
                'sr':   round(new_risk, 1),
                'rd':   row.get('risques_detail') if not pd.isna(row.get('risques_detail') or float('nan')) else None,
                'sg':   score_global,
                'l':    lettre,
                'nb':   nb_cat,
                'now':  datetime.utcnow(),
                'code': code,
            })
            count += 1
            if count % 5000 == 0:
                await session.commit()
                print(f"  → {count:,}/{len(df):,} communes mises à jour")

        await session.commit()

    print(f"\nTerminé. {count:,} communes mises à jour avec score_risques.")


if __name__ == "__main__":
    asyncio.run(run())
