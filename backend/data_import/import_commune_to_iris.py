"""
Enrichit les scores IRIS avec les sous-scores communaux.

Les données sécurité, transports et éducation ne sont disponibles qu'au niveau commune.
Tous les IRIS d'une même commune partagent ces 3 valeurs (transparent et honnête).

De plus, si le score_sante d'un IRIS est 0 ou absent mais que la commune a une APL > 0,
on applique le score santé de la commune comme fallback (les consultations APL couvrent
une aire de chalandise, pas une zone IRIS précise).

Doit être lancé APRÈS : import_bpe_iris, import_dvf_iris, import_filosofi_iris,
                         import_transports (communes), import_securite, import_education.

Usage : python3 -m backend.data_import.import_commune_to_iris
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global


async def run():
    print("=== Enrichissement IRIS avec scores communes ===\n")
    await init_db()

    async with async_session() as session:
        # ── 1. Charger les scores communes ──────────────────────────────────
        result = await session.execute(text("""
            SELECT code_insee,
                   score_securite, score_transports, score_education, score_sante
            FROM scores
        """))
        commune_scores = {}
        for r in result.fetchall():
            commune_scores[r[0]] = {
                'securite':   float(r[1]) if r[1] is not None and r[1] >= 0 else -1,
                'transports': float(r[2]) if r[2] is not None and r[2] >= 0 else -1,
                'education':  float(r[3]) if r[3] is not None and r[3] >= 0 else -1,
                'sante':      float(r[4]) if r[4] is not None and r[4] >= 0 else -1,
            }
        print(f"  {len(commune_scores)} communes chargées")

        # ── 2. Charger tous les IRIS déjà scorés localement ─────────────────
        # On n'enrichit que les IRIS qui ont déjà au moins 1 score local (equip/sante/immo).
        # Les IRIS sans aucune donnée locale ne reçoivent pas de score basé uniquement
        # sur les données communes (ce serait trompeur : ils auraient l'air scorés sans données propres).
        result = await session.execute(text("""
            SELECT iz.code_iris, iz.code_commune,
                   s.score_equipements, s.score_sante, s.score_immobilier
            FROM iris_zones iz
            INNER JOIN iris_scores s ON iz.code_iris = s.code_iris
        """))
        iris_rows = result.fetchall()
        print(f"  {len(iris_rows)} IRIS à traiter")

        # ── 3. Enrichissement + recalcul ────────────────────────────────────
        nb_updated = 0
        nb_sante_fallback = 0
        nb_with_commune_data = 0

        batches = []
        for row in iris_rows:
            code_iris, code_commune = row[0], row[1]

            # Sous-scores IRIS locaux
            seq  = float(row[2]) if row[2] is not None and row[2] >= 0 else -1.0
            sante = float(row[3]) if row[3] is not None and row[3] >= 0 else -1.0
            simmo = float(row[4]) if row[4] is not None and row[4] >= 0 else -1.0

            # Scores depuis la commune
            cs = commune_scores.get(code_commune, {})
            s_secu  = cs.get('securite', -1)
            s_trans = cs.get('transports', -1)
            s_edu   = cs.get('education', -1)
            s_sante_commune = cs.get('sante', -1)

            # Fallback santé : si IRIS n'a pas de score santé mais la commune si
            if (sante <= 0) and s_sante_commune > 0:
                sante = round(s_sante_commune, 1)
                nb_sante_fallback += 1

            if any(v >= 0 for v in [s_secu, s_trans, s_edu]):
                nb_with_commune_data += 1

            # Recalcul score global avec toutes les catégories disponibles
            sous_scores = {}
            if seq >= 0:    sous_scores['equipements'] = seq
            if sante >= 0:  sous_scores['sante'] = sante
            if simmo >= 0:  sous_scores['immobilier'] = simmo
            if s_secu >= 0:  sous_scores['securite'] = s_secu
            if s_trans >= 0: sous_scores['transports'] = s_trans
            if s_edu >= 0:   sous_scores['education'] = s_edu

            if not sous_scores:
                continue

            score, lettre, nb_cat = calculer_score_global(sous_scores)

            batches.append({
                'ci': code_iris,
                'sg': score, 'l': lettre, 'nb': nb_cat,
                'seq':   seq,
                'ss':    sante,
                'simmo': simmo,
                'ssecu': s_secu,
                'strans': s_trans,
                'sedu':  s_edu,
            })

        print(f"  → {nb_with_commune_data} IRIS enrichis avec données communes")
        print(f"  → {nb_sante_fallback} IRIS avec fallback santé commune")
        print(f"\nSauvegarde {len(batches)} IRIS en base...")

        # ── 4. Upsert en batches ─────────────────────────────────────────────
        for i, params in enumerate(batches):
            await session.execute(text("""
                UPDATE iris_scores SET
                    score_sante      = :ss,
                    score_securite   = :ssecu,
                    score_transports = :strans,
                    score_education  = :sedu,
                    score_global     = :sg,
                    lettre           = :l,
                    nb_categories_scorees = :nb,
                    updated_at       = CURRENT_TIMESTAMP
                WHERE code_iris = :ci
            """), params)
            nb_updated += 1
            if nb_updated % 5000 == 0:
                await session.commit()
                print(f"  → {nb_updated}/{len(batches)} IRIS sauvegardés")

        await session.commit()

    print(f"\n  → {nb_updated} IRIS mis à jour au total")

    # ── 5. Stats finales ─────────────────────────────────────────────────────
    print("\nStats après enrichissement :")
    import sqlite3
    import pathlib
    db_path = pathlib.Path(__file__).parent.parent.parent / 'vivreici.db'
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN nb_categories_scorees >= 2 THEN 1 END) as avec_lettre,
            COUNT(CASE WHEN nb_categories_scorees >= 4 THEN 1 END) as enrichis_4cat,
            COUNT(CASE WHEN nb_categories_scorees >= 6 THEN 1 END) as enrichis_6cat,
            AVG(CASE WHEN nb_categories_scorees >= 2 THEN score_global END) as score_moyen,
            COUNT(CASE WHEN lettre = 'A' THEN 1 END) as nb_A,
            COUNT(CASE WHEN lettre = 'B' THEN 1 END) as nb_B,
            COUNT(CASE WHEN lettre = 'C' THEN 1 END) as nb_C,
            COUNT(CASE WHEN lettre = 'D' THEN 1 END) as nb_D,
            COUNT(CASE WHEN lettre = 'E' THEN 1 END) as nb_E
        FROM iris_scores
    """).fetchone()
    total, avec_lettre, enr4, enr6, score_moy, nA, nB, nC, nD, nE = rows
    print(f"  Total IRIS scorés : {total:,}")
    print(f"  Avec lettre (>=2 cats) : {avec_lettre:,} ({100*avec_lettre/total:.0f}%)")
    print(f"  Enrichis >=4 catégories : {enr4:,}")
    print(f"  Enrichis >=6 catégories : {enr6:,}")
    print(f"  Score moyen : {score_moy:.1f}")
    print(f"  Distribution : A={nA} B={nB} C={nC} D={nD} E={nE}")

    # Quelques exemples pour validation
    print("\nExemples Limoges (87085) :")
    for row in conn.execute("""
        SELECT iz.nom, s.score_global, s.lettre, s.nb_categories_scorees,
               s.score_securite, s.score_transports, s.score_education,
               s.score_equipements, s.score_sante, s.score_immobilier
        FROM iris_zones iz
        JOIN iris_scores s ON iz.code_iris = s.code_iris
        WHERE iz.code_commune = '87085'
        ORDER BY s.score_global DESC
        LIMIT 6
    """).fetchall():
        print(f"  {row[0]:<35} → {row[2]} ({row[1]:.1f}) [{row[3]} cats] "
              f"eq={row[7] or -1:.0f} sa={row[8] or -1:.0f} im={row[9] or -1:.0f} "
              f"sc={row[4] or -1:.0f} tr={row[5] or -1:.0f} ed={row[6] or -1:.0f}")

    print("\nScore commune Limoges pour référence :")
    for row in conn.execute("""
        SELECT s.score_global, s.lettre, s.score_securite, s.score_transports, s.score_education
        FROM scores s WHERE s.code_insee = '87085'
    """).fetchall():
        print(f"  Limoges : {row[1]} ({row[0]:.1f}) sc={row[2] or -1:.0f} tr={row[3] or -1:.0f} ed={row[4] or -1:.0f}")

    conn.close()
    print("\n=== Import commune→IRIS terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
