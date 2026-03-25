"""
Génère tous les fichiers JSON statiques pour le déploiement full-static.
Lance depuis /Users/admin/vivreici/ :
  python3 scripts/export_all_static.py

Produit :
  frontend/public/communes-map.json       — carte + search + classement + geo
  frontend/public/iris-locator.json       — index IRIS léger pour GPS locate
  frontend/public/data/stats.json         — compteurs globaux
  frontend/public/data/communes/{code}.json — fiche commune complète
  frontend/public/data/iris/{code}.json   — fiche IRIS complète
  frontend/public/data/iris-map/{code_commune}.json — IRIS avec géométrie par commune
"""
import sqlite3
import json
import os
import pathlib
import sys

DB_PATH = pathlib.Path(__file__).parent.parent / 'vivreici.db'
PUBLIC = pathlib.Path(__file__).parent.parent / 'frontend' / 'public'
DATA = PUBLIC / 'data'

PLM_PARENTS = {'75056', '69123', '13055'}
VALID_LETTRES = ('A', 'B', 'C', 'D', 'E')

def mkdirs():
    (DATA / 'communes').mkdir(parents=True, exist_ok=True)
    (DATA / 'iris').mkdir(parents=True, exist_ok=True)
    (DATA / 'iris-map').mkdir(parents=True, exist_ok=True)

def dump(path, obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, separators=(',', ':'), ensure_ascii=False)

def lettre_ok(l):
    return l if l in VALID_LETTRES else None

def safe_json(s):
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

def main():
    if not DB_PATH.exists():
        print(f"ERREUR: DB non trouvée à {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    mkdirs()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    print("Chargement des données...")

    # ── 1. Communes ──────────────────────────────────────────────────────────
    communes_rows = conn.execute("""
        SELECT c.code_insee, c.nom, c.departement, c.region, c.population,
               c.codes_postaux, c.latitude, c.longitude
        FROM communes c
        WHERE c.latitude IS NOT NULL
    """).fetchall()
    communes_map = {r['code_insee']: dict(r) for r in communes_rows}

    # ── 2. Scores ──────────────────────────────────────────────────────────
    scores_rows = conn.execute("""
        SELECT code_insee, score_global, lettre, nb_categories_scorees,
               score_equipements, score_securite, score_immobilier, score_education,
               score_sante, score_transports, score_environnement, score_demographie,
               nb_equipements, apl_medecins, taux_criminalite,
               prix_m2_median, prix_m2_median_2022, nb_gares, distance_gare_km,
               nb_arrets_tc, equipements_detail, poi_detail,
               nom_gare, transport_detail,
               evolution_population_5ans, taux_pauvrete, updated_at
        FROM scores
    """).fetchall()
    scores_map = {r['code_insee']: dict(r) for r in scores_rows}

    def score_to_dict(s):
        def sub(val):
            return val if val is not None and val >= 0 else None
        return {
            'score_global': s['score_global'],
            'lettre': lettre_ok(s['lettre']),
            'sous_scores': {
                'equipements':  sub(s['score_equipements']),
                'securite':     sub(s['score_securite']),
                'immobilier':   sub(s['score_immobilier']),
                'education':    sub(s['score_education']),
                'sante':        sub(s['score_sante']),
                'transports':   sub(s['score_transports']),
                'environnement':sub(s['score_environnement']),
                'demographie':  sub(s['score_demographie']),
            },
            'donnees_brutes': {
                'nb_equipements':        s['nb_equipements'],
                'apl_medecins':          s['apl_medecins'] if s['apl_medecins'] and s['apl_medecins'] >= 0 else None,
                'taux_criminalite':      s['taux_criminalite'],
                'prix_m2_median':        s['prix_m2_median'],
                'prix_m2_median_2022':   s['prix_m2_median_2022'],
                'nb_gares':              s['nb_gares'] or 0,
                'distance_gare_km':      s['distance_gare_km'] if s['distance_gare_km'] and s['distance_gare_km'] >= 0 else None,
                'nom_gare':              s['nom_gare'] if s['nom_gare'] else None,
                'transport_detail':      safe_json(s['transport_detail']),
                'equipements_detail':    safe_json(s['equipements_detail']),
                'poi_detail':            safe_json(s['poi_detail']),
                'evolution_population_5ans': s['evolution_population_5ans'],
                'taux_pauvrete':         s['taux_pauvrete'],
            },
            'nb_categories_scorees': s['nb_categories_scorees'],
            'updated_at': s['updated_at'],
        }

    # ── 3. IRIS zones + scores ───────────────────────────────────────────────
    print("Chargement IRIS...")
    iris_rows = conn.execute("""
        SELECT iz.code_iris, iz.nom, iz.code_commune, iz.typ_iris,
               iz.population, iz.latitude, iz.longitude, iz.geometry,
               s.score_global, s.lettre, s.nb_categories_scorees,
               s.score_equipements, s.score_sante, s.score_immobilier, s.score_revenus,
               s.nb_equipements, s.nb_medecins_pour_10000,
               s.prix_m2_median, s.revenu_median, s.taux_pauvrete,
               s.equipements_detail, s.poi_detail
        FROM iris_zones iz
        LEFT JOIN iris_scores s ON iz.code_iris = s.code_iris
    """).fetchall()
    iris_map = {}
    iris_by_commune = {}  # code_commune → list of iris rows
    for r in iris_rows:
        d = dict(r)
        iris_map[d['code_iris']] = d
        cc = d['code_commune']
        if cc not in iris_by_commune:
            iris_by_commune[cc] = []
        iris_by_commune[cc].append(d)

    def iris_lettre(r):
        if r is None:
            return None
        l = r.get('lettre')
        if l not in VALID_LETTRES:
            return None
        if (r.get('nb_categories_scorees') or 0) < 2:
            return None
        return l

    # ── 4. Stats ─────────────────────────────────────────────────────────────
    nb_scorees = sum(1 for s in scores_map.values() if s['nb_categories_scorees'] >= 1)
    nb_iris_total = len(iris_map)
    stats = {
        'nb_communes': len(communes_map),
        'nb_scorees': nb_scorees,
        'nb_iris': nb_iris_total,
        'categories': ['equipements', 'sante', 'securite', 'immobilier', 'education',
                       'transports', 'environnement', 'demographie'],
    }
    dump(DATA / 'stats.json', stats)
    print(f"  stats.json OK")

    # ── 5. communes-map.json (slim — carte + recherche de base) ─────────────
    # Ordre des sous_scores pour communes-scores.json (compact array)
    SS_ORDER = ['equipements', 'securite', 'immobilier', 'education', 'sante', 'transports', 'environnement', 'demographie']
    SS_COLS  = ['score_equipements', 'score_securite', 'score_immobilier', 'score_education',
                'score_sante', 'score_transports', 'score_environnement', 'score_demographie']

    print("Génération communes-map.json (slim)...")
    map_communes = []
    scores_compact = []  # [[code_insee, [ss...], prix_m2], ...]

    for code, c in communes_map.items():
        if code in PLM_PARENTS:
            continue
        s = scores_map.get(code)
        if not s or s['nb_categories_scorees'] < 3:
            continue
        if not c['latitude'] or not c['longitude']:
            continue
        map_communes.append({
            'code_insee':   code,
            'nom':          c['nom'],
            'departement':  c['departement'],
            'region':       c['region'],
            'population':   c['population'],
            'codes_postaux': c['codes_postaux'].split(',') if c['codes_postaux'] else [],
            'latitude':     round(c['latitude'], 5),
            'longitude':    round(c['longitude'], 5),
            'score_global': round(s['score_global'], 1) if s['score_global'] is not None else None,
            'lettre':       lettre_ok(s['lettre']),
        })
        # Scores compacts : [code_insee, [ss_array], prix_m2]
        ss = []
        for col in SS_COLS:
            v = s[col]
            ss.append(round(v) if v is not None and v >= 0 else None)
        scores_compact.append([code, ss, round(s['prix_m2_median']) if s['prix_m2_median'] else None])

    map_communes.sort(key=lambda x: -(x['population'] or 0))
    dump(PUBLIC / 'communes-map.json', map_communes)
    print(f"  communes-map.json: {len(map_communes)} communes")

    # ── 5b. communes-scores.json (lazy — sous_scores + prix_m2 pour recherche/classement) ──
    dump(PUBLIC / 'communes-scores.json', scores_compact)
    print(f"  communes-scores.json: {len(scores_compact)} entrées")

    # ── 6. iris-locator.json ─────────────────────────────────────────────────
    print("Génération iris-locator.json...")
    locator = []
    for r in iris_rows:
        if r['latitude'] and r['longitude']:
            locator.append([r['code_iris'], r['code_commune'], round(r['latitude'], 5), round(r['longitude'], 5)])
    dump(PUBLIC / 'iris-locator.json', locator)
    print(f"  iris-locator.json: {len(locator)} IRIS")

    # ── 7. Rang par département ──────────────────────────────────────────────
    print("Calcul des rangs par département...")
    dept_scores = {}
    for code, s in scores_map.items():
        if code in PLM_PARENTS:
            continue
        if s['nb_categories_scorees'] < 3 or s['score_global'] is None:
            continue
        c = communes_map.get(code)
        if not c:
            continue
        dept = c['departement']
        if dept not in dept_scores:
            dept_scores[dept] = []
        dept_scores[dept].append((code, s['score_global']))

    for dept in dept_scores:
        dept_scores[dept].sort(key=lambda x: -x[1])

    rang_dept = {}  # code_insee → (rang, nb_total)
    for dept, items in dept_scores.items():
        for i, (code, _) in enumerate(items):
            rang_dept[code] = (i + 1, len(items))

    # ── 8. Commune detail files ──────────────────────────────────────────────
    print("Génération fichiers communes...")
    nb_communes_written = 0
    for code, c in communes_map.items():
        s = scores_map.get(code)
        obj = {
            'code_insee':   code,
            'nom':          c['nom'],
            'departement':  c['departement'],
            'region':       c['region'],
            'population':   c['population'],
            'codes_postaux': c['codes_postaux'].split(',') if c['codes_postaux'] else [],
            'latitude':     c['latitude'],
            'longitude':    c['longitude'],
            'score':        score_to_dict(s) if s else None,
        }
        # Rang département
        if code in rang_dept:
            obj['rang_departement'] = rang_dept[code][0]
            obj['nb_communes_departement'] = rang_dept[code][1]

        # Liste IRIS (si plusieurs)
        commune_iris = iris_by_commune.get(code, [])
        # Aussi gérer Paris/Lyon/Marseille (PLM) : IRIS avec code_commune = arrondissements
        # Pour les PLM parents, rien (les pages IRIS sont sur les arrondissements)

        # Filtrer IRIS type Z si commune a aussi des H/A/D
        non_z = [r for r in commune_iris if r['typ_iris'] != 'Z']
        iris_to_show = non_z if non_z else commune_iris

        if len(iris_to_show) > 1:
            # Trier : complètes d'abord, puis partielles
            def iris_sort_key(r):
                nb_cat = r.get('nb_categories_scorees') or 0
                score = r.get('score_global') or -1
                return (0 if nb_cat >= 2 else 1, -score)
            iris_to_show_sorted = sorted(iris_to_show, key=iris_sort_key)

            complete = [(i, r) for i, r in enumerate(iris_to_show_sorted)
                        if (r.get('nb_categories_scorees') or 0) >= 2]
            nb_total = len(iris_to_show_sorted)
            nb_scored = len(complete)

            iris_list = []
            rang_counter = 0
            for r in iris_to_show_sorted:
                est_partiel = (r.get('nb_categories_scorees') or 0) < 2
                if not est_partiel:
                    rang_counter += 1
                    rang = rang_counter
                else:
                    rang = None
                def sub_i(val):
                    return val if val is not None and val >= 0 else None
                iris_list.append({
                    'code_iris':   r['code_iris'],
                    'nom':         r['nom'],
                    'code_commune': r['code_commune'],
                    'typ_iris':    r['typ_iris'],
                    'population':  r['population'],
                    'latitude':    r['latitude'],
                    'longitude':   r['longitude'],
                    'score_global': r['score_global'],
                    'lettre':       iris_lettre(r),
                    'nb_categories_scorees': r.get('nb_categories_scorees') or 0,
                    'donnees_partielles': est_partiel,
                    'rang':         rang,
                    'nb_total':     nb_total,
                    'nb_scored':    nb_scored,
                    'sous_scores': {
                        'equipements': sub_i(r.get('score_equipements')),
                        'sante':       sub_i(r.get('score_sante')),
                        'immobilier':  sub_i(r.get('score_immobilier')),
                        'revenus':     sub_i(r.get('score_revenus')),
                    },
                })
            obj['iris'] = iris_list
        else:
            obj['iris'] = []

        dump(DATA / 'communes' / f'{code}.json', obj)
        nb_communes_written += 1
        if nb_communes_written % 5000 == 0:
            print(f"  {nb_communes_written} communes écrites...")

    print(f"  Total: {nb_communes_written} fichiers communes")

    # ── 9. IRIS detail files ─────────────────────────────────────────────────
    print("Génération fichiers IRIS...")
    # Rang par commune (basé sur iris_by_commune)
    rang_iris = {}  # code_iris → (rang, nb_total)
    for cc, rows in iris_by_commune.items():
        complete = [r for r in rows if r['typ_iris'] != 'Z' and (r.get('nb_categories_scorees') or 0) >= 2]
        if not complete:
            continue
        complete.sort(key=lambda r: -(r.get('score_global') or -1))
        for i, r in enumerate(complete):
            rang_iris[r['code_iris']] = (i + 1, len(complete))

    nb_iris_written = 0
    for code_iris, r in iris_map.items():
        s_global = r.get('score_global')
        lettre = iris_lettre(r)

        def sub_iris(val):
            return val if val is not None and val >= 0 else None

        score_obj = {
            'score_global': s_global,
            'lettre': lettre,
            'sous_scores': {
                'equipements': sub_iris(r.get('score_equipements')),
                'sante':       sub_iris(r.get('score_sante')),
                'immobilier':  sub_iris(r.get('score_immobilier')),
                'revenus':     sub_iris(r.get('score_revenus')),
            },
            'donnees_brutes': {
                'nb_equipements':        r.get('nb_equipements'),
                'medecins_pour_10000':   r.get('nb_medecins_pour_10000'),
                'prix_m2_median':        r.get('prix_m2_median'),
                'revenu_median':         r.get('revenu_median'),
                'taux_pauvrete':         r.get('taux_pauvrete'),
                'equipements_detail':    safe_json(r.get('equipements_detail')),
                'poi_detail':            safe_json(r.get('poi_detail')),
            },
            'nb_categories_scorees': r.get('nb_categories_scorees') or 0,
        } if r.get('nb_categories_scorees') else None

        code_commune = r['code_commune']
        commune = communes_map.get(code_commune)
        commune_nom = commune['nom'] if commune else code_commune

        rang_info = rang_iris.get(code_iris, (None, 0))
        obj = {
            'code_iris':      code_iris,
            'nom':            r['nom'],
            'code_commune':   code_commune,
            'typ_iris':       r['typ_iris'],
            'population':     r['population'],
            'latitude':       r['latitude'],
            'longitude':      r['longitude'],
            'score':          score_obj,
            'rang_commune':   rang_info[0],
            'nb_iris_commune': rang_info[1],
            'commune_nom':    commune_nom,
        }
        dump(DATA / 'iris' / f'{code_iris}.json', obj)
        nb_iris_written += 1
        if nb_iris_written % 10000 == 0:
            print(f"  {nb_iris_written} IRIS écrits...")

    print(f"  Total: {nb_iris_written} fichiers IRIS")

    # ── 10. iris-map par commune ─────────────────────────────────────────────
    print("Génération fichiers iris-map...")
    nb_map_written = 0
    for code_commune, rows in iris_by_commune.items():
        features = []
        for r in rows:
            geo = safe_json(r.get('geometry'))
            if not geo:
                # Fallback: point centroïde si pas de géométrie
                if r['latitude'] and r['longitude']:
                    geo = {'type': 'Point', 'coordinates': [r['longitude'], r['latitude']]}
                else:
                    continue
            features.append({
                'type': 'Feature',
                'geometry': geo,
                'properties': {
                    'code_iris':    r['code_iris'],
                    'nom':          r['nom'],
                    'typ_iris':     r['typ_iris'],
                    'score_global': r.get('score_global'),
                    'lettre':       iris_lettre(r),
                    'population':   r['population'],
                },
            })
        if features:
            dump(DATA / 'iris-map' / f'{code_commune}.json', {
                'type': 'FeatureCollection',
                'features': features,
            })
            nb_map_written += 1

    print(f"  Total: {nb_map_written} fichiers iris-map")
    conn.close()

    # Tailles
    total = sum(
        os.path.getsize(DATA / d / f)
        for d in ['communes', 'iris', 'iris-map']
        for f in os.listdir(DATA / d)
    )
    map_size = os.path.getsize(PUBLIC / 'communes-map.json')
    loc_size = os.path.getsize(PUBLIC / 'iris-locator.json')
    print(f"\nTaille totale data/ : {total / 1024 / 1024:.0f} MB")
    print(f"communes-map.json   : {map_size / 1024:.0f} KB")
    print(f"iris-locator.json   : {loc_size / 1024:.0f} KB")
    print("\nDone.")

if __name__ == '__main__':
    main()
