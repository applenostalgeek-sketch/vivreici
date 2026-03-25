"""
Import TC GTFS national — version optimisée multi-thread.

Optimisations :
- 30 téléchargements concurrents (réseau saturé)
- Parsing dans un ThreadPoolExecutor (4 cœurs utilisés)
- csv.reader natif au lieu de pandas pour stop_times (3-5× plus rapide)
- Pas de parsing calendar.txt (on compte tous les trips — approximation valide,
  les ratios relatifs entre communes sont conservés)
- Timeout court (25s) pour sauter les serveurs morts
"""

import asyncio
import csv
import io
import json
import os
import sys
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from scipy.spatial import cKDTree
from sqlalchemy import text
from backend.database import async_session, init_db
from backend.scoring import calculer_score_global, percentile_to_score

LAT_MIN, LAT_MAX = 41.0, 52.0
LON_MIN, LON_MAX = -6.0, 10.0
MAX_CONCURRENT = 30
FEED_TIMEOUT = 25


# ── Fetch liste des feeds ──────────────────────────────────────────────────────

async def get_all_gtfs_feeds() -> list[dict]:
    print("Récupération des feeds GTFS...")
    feeds = []
    SKIP = {"Renfe", "FlixBus", "Eurostar", "Trenitalia", "BlaBlaCar"}
    # L'API retourne tous les feeds en une seule réponse (page_size ignoré)
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            "https://transport.data.gouv.fr/api/datasets"
            "?type=public-transit&format=GTFS&page_size=10000"
        )
        data = r.json() if r.status_code == 200 else []
        for feed in data:
            title = feed.get("title", "")
            if any(s in title for s in SKIP):
                continue
            for res in feed.get("resources", []):
                if res.get("format", "").upper() == "GTFS":
                    url = res.get("original_url") or res.get("url", "")
                    if url.startswith("http"):
                        feeds.append({"name": title[:50], "url": url})
                        break
    print(f"  → {len(feeds)} feeds trouvés")
    return feeds


# ── Parsing d'un zip GTFS (CPU-bound, lancé dans un thread) ───────────────────

# Poids par route_type GTFS — reflète l'utilité réelle pour les déplacements quotidiens
# Métro/RER >> Tram > Bus urbain = Bus interurbain (on ne peut pas distinguer depuis route_type seul)
ROUTE_WEIGHTS = {
    0: 3.0,   # Tram / tramway
    1: 5.0,   # Métro / subway
    2: 4.0,   # Rail (RER, TER, Intercités)
    3: 1.0,   # Bus (urbain et interurbain — même type, pas de distinction possible)
    4: 1.5,   # Ferry
    5: 2.0,   # Cable tram
    6: 1.5,   # Aerial lift
    7: 1.5,   # Funicular
    11: 2.0,  # Trolleybus
    12: 3.0,  # Monorail
}
DEFAULT_WEIGHT = 1.0


def parse_zip(zip_bytes: bytes) -> tuple[dict, dict]:
    """
    Parse un zip GTFS.
    Retourne :
    - trips_dict  : {(rlat, rlon): trips_pondérés}
    - routes_dict : {(rlat, rlon): set de (type_code, short_name, long_name)}

    Pondération par route_type :
    - Métro × 5, Rail/RER × 4, Tram × 3, Bus × 1
    - Un arrêt de métro parisien (500 trips × 5) >> un car rural (30 trips × 1)
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = set(zf.namelist())

            def find(fname):
                for n in names:
                    if n == fname or n.endswith("/" + fname):
                        return n
                return None

            stops_f = find("stops.txt")
            times_f = find("stop_times.txt")
            trips_f = find("trips.txt")
            routes_f = find("routes.txt")
            if not stops_f or not times_f:
                return {}, {}

            # routes.txt → route_id → poids + métadonnées (type, short, long)
            route_weight: dict[str, float] = {}
            route_meta: dict[str, tuple] = {}  # route_id → (type_code, short_name, long_name)
            if routes_f:
                with zf.open(routes_f) as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                    header = next(reader, None)
                    if header:
                        header = [h.strip() for h in header]
                        try:
                            ri  = header.index("route_id")
                            rti = header.index("route_type")
                            sni = header.index("route_short_name") if "route_short_name" in header else -1
                            lni = header.index("route_long_name")  if "route_long_name"  in header else -1
                            for row in reader:
                                if len(row) > max(ri, rti):
                                    try:
                                        rt    = int(row[rti].strip())
                                        short = row[sni].strip()[:30] if sni >= 0 and len(row) > sni else ""
                                        long_ = row[lni].strip()[:80] if lni >= 0 and len(row) > lni else ""
                                        route_weight[row[ri]] = ROUTE_WEIGHTS.get(rt, DEFAULT_WEIGHT)
                                        route_meta[row[ri]]   = (rt, short, long_)
                                    except ValueError:
                                        pass
                        except ValueError:
                            pass

            # trips.txt → trip_id → poids + route_id
            trip_weight: dict[str, float] = {}
            trip_route_id: dict[str, str] = {}
            if trips_f:
                with zf.open(trips_f) as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                    header = next(reader, None)
                    if header:
                        header = [h.strip() for h in header]
                        try:
                            tid_i = header.index("trip_id")
                            rid_i = header.index("route_id")
                            for row in reader:
                                if len(row) > max(tid_i, rid_i):
                                    rid = row[rid_i]
                                    tid = row[tid_i]
                                    trip_weight[tid]   = route_weight.get(rid, DEFAULT_WEIGHT)
                                    trip_route_id[tid] = rid
                        except ValueError:
                            pass

            # stop_times.txt → {stop_id: {trip_id: weight}} + {stop_id: set(route_id)}
            stop_trips: dict[str, dict[str, float]] = defaultdict(dict)
            stop_route_ids: dict[str, set] = defaultdict(set)
            with zf.open(times_f) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                header = next(reader, None)
                if not header:
                    return {}, {}
                header = [h.strip() for h in header]
                try:
                    si = header.index("stop_id")
                    ti = header.index("trip_id")
                except ValueError:
                    return {}, {}
                for row in reader:
                    if len(row) > max(si, ti):
                        tid = row[ti]
                        sid = row[si]
                        if tid not in stop_trips[sid]:
                            stop_trips[sid][tid] = trip_weight.get(tid, DEFAULT_WEIGHT)
                        rid = trip_route_id.get(tid)
                        if rid and rid in route_meta:
                            stop_route_ids[sid].add(rid)

            # stops.txt → lat/lon → résultat pondéré + routes
            result: dict = {}
            routes_result: dict = {}
            with zf.open(stops_f) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                header = next(reader, None)
                if not header:
                    return {}, {}
                header = [h.strip() for h in header]
                try:
                    id_i  = header.index("stop_id")
                    lat_i = header.index("stop_lat")
                    lon_i = header.index("stop_lon")
                except ValueError:
                    return {}, {}
                for row in reader:
                    if len(row) <= max(id_i, lat_i, lon_i):
                        continue
                    try:
                        lat = float(row[lat_i])
                        lon = float(row[lon_i])
                    except ValueError:
                        continue
                    if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
                        continue
                    sid = row[id_i]
                    weighted = sum(stop_trips.get(sid, {}).values())
                    if weighted == 0:
                        continue
                    pos = (round(lat, 3), round(lon, 3))
                    if weighted > result.get(pos, 0):
                        result[pos] = weighted
                    # Lignes desservant cette position
                    if sid in stop_route_ids:
                        if pos not in routes_result:
                            routes_result[pos] = set()
                        routes_result[pos].update(route_meta[rid] for rid in stop_route_ids[sid])
            return result, routes_result
    except Exception:
        return {}, {}


# ── Pipeline async download + thread parse ────────────────────────────────────

async def run():
    print("=== Import TC GTFS national — version optimisée ===\n")
    await init_db()

    # Charger communes
    async with async_session() as session:
        res = await session.execute(text("""
            SELECT c.code_insee, c.latitude, c.longitude
            FROM communes c JOIN scores s ON s.code_insee = c.code_insee
            WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL
        """))
        rows = res.fetchall()
    codes     = [r[0] for r in rows]
    comm_lats = np.array([float(r[1]) for r in rows])
    comm_lons = np.array([float(r[2]) for r in rows])
    print(f"  {len(codes)} communes chargées\n")

    feeds = await get_all_gtfs_feeds()

    # Résultat global : (rlat, rlon) → max trips + union des routes par position
    global_trips: dict = {}
    global_routes: dict = {}  # (rlat, rlon) → set de (type_code, short, long)
    counters = {"ok": 0, "err": 0, "empty": 0, "done": 0, "total": len(feeds)}

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    executor = ThreadPoolExecutor(max_workers=4)
    loop = asyncio.get_event_loop()

    print(f"Téléchargement + parsing ({MAX_CONCURRENT} concurrent, 4 threads CPU)...\n")

    async def process(client, feed):
        async with sem:
            try:
                r = await client.get(feed["url"], timeout=FEED_TIMEOUT)
                if r.status_code != 200:
                    counters["err"] += 1
                    counters["done"] += 1
                    return
                data = r.content
            except Exception:
                counters["err"] += 1
                counters["done"] += 1
                return

        # Parsing dans un thread (libère l'event loop)
        try:
            stops, routes = await loop.run_in_executor(executor, parse_zip, data)
        except Exception:
            stops, routes = {}, {}

        if not stops:
            counters["empty"] += 1
        else:
            for pos, trips in stops.items():
                if trips > global_trips.get(pos, 0):
                    global_trips[pos] = trips
            for pos, rset in routes.items():
                if pos not in global_routes:
                    global_routes[pos] = set()
                global_routes[pos].update(rset)
            counters["ok"] += 1

        counters["done"] += 1
        if counters["done"] % 50 == 0 or counters["done"] == counters["total"]:
            pct = 100 * counters["done"] // counters["total"]
            print(f"  [{pct:3d}%] {counters['done']}/{counters['total']} — "
                  f"{counters['ok']} OK, {counters['err']} err, {counters['empty']} vides — "
                  f"{len(global_trips):,} positions")

    async with httpx.AsyncClient(follow_redirects=True, timeout=FEED_TIMEOUT) as client:
        await asyncio.gather(*[process(client, f) for f in feeds])

    executor.shutdown(wait=False)

    print(f"\n  {len(global_trips):,} positions GTFS uniques")
    trips_vals = np.array(list(global_trips.values()))
    print(f"  Distribution trips : médiane={np.median(trips_vals):.0f}, "
          f"p90={np.percentile(trips_vals,90):.0f}, max={trips_vals.max():.0f}")

    # Assignation aux communes (KDTree + Voronoi)
    print("\nAssignation aux communes...")
    R = 1.0
    xs = R * np.cos(np.radians(comm_lats)) * np.cos(np.radians(comm_lons))
    ys = R * np.cos(np.radians(comm_lats)) * np.sin(np.radians(comm_lons))
    zs = R * np.sin(np.radians(comm_lats))
    coords = np.column_stack([xs, ys, zs])
    tree = cKDTree(coords)
    self_dists, _ = tree.query(coords, k=2)
    max_dists_sphere = np.clip(self_dists[:, 1] * 6371.0 * 0.5, 1.0, 5.0) / 6371.0

    positions = list(global_trips.keys())
    rlats = np.radians([p[0] for p in positions])
    rlons = np.radians([p[1] for p in positions])
    xs_q = R * np.cos(rlats) * np.cos(rlons)
    ys_q = R * np.cos(rlats) * np.sin(rlons)
    zs_q = R * np.sin(rlats)
    dists, idxs = tree.query(np.column_stack([xs_q, ys_q, zs_q]))

    commune_trips: dict[str, int] = {}
    commune_routes: dict[str, set] = defaultdict(set)
    for i, pos in enumerate(positions):
        ci = idxs[i]
        if dists[i] <= max_dists_sphere[ci]:
            c = codes[ci]
            commune_trips[c] = commune_trips.get(c, 0) + global_trips[pos]
            if pos in global_routes:
                commune_routes[c].update(global_routes[pos])

    trips_arr = np.array([commune_trips.get(c, 0) for c in codes])
    nonzero = trips_arr[trips_arr > 0]
    print(f"  {len(commune_trips):,} communes avec TC")
    print(f"  Médiane TC : {np.median(nonzero):.0f} trips/jour, max : {trips_arr.max():.0f}")

    # Résultats test
    test = {"28052":"Bouglainval","78517":"Rambouillet",
            "75101":"Paris 1er","75110":"Paris 10e","75115":"Paris 15e"}
    print("\n  Communes test :")
    for code, label in test.items():
        print(f"    {label:<20} : {commune_trips.get(code, 0):>8,} trips/jour")

    # Score transport
    print("\nRecalcul scores transport...")
    async with async_session() as session:
        res = await session.execute(text(
            "SELECT code_insee, distance_gare_km FROM scores"
        ))
        score_rows = res.fetchall()

    codes_s  = [r[0] for r in score_rows]
    dist_arr = np.array([float(r[1]) if r[1] is not None and float(r[1]) >= 0 else np.nan
                         for r in score_rows])
    tc_arr   = np.array([commune_trips.get(c, 0) for c in codes_s], dtype=float)

    serie_dist = pd.Series(dist_arr[~np.isnan(dist_arr)])
    serie_tc   = pd.Series(tc_arr)

    scores_gare = np.array([
        percentile_to_score(d, serie_dist, "inverse") if not np.isnan(d) else -1.0
        for d in dist_arr
    ])
    scores_tc = np.array([
        percentile_to_score(t, serie_tc, "direct") for t in tc_arr
    ])

    final_scores = np.where(
        (scores_gare >= 0) & (scores_tc >= 0), np.round(0.5*scores_gare + 0.5*scores_tc, 1),
        np.where(scores_gare >= 0, scores_gare,
        np.where(scores_tc >= 0, scores_tc, -1.0))
    )

    valid = final_scores >= 0
    print(f"  Score médian : {np.median(final_scores[valid]):.1f} — {valid.sum():,} communes")

    print("\n  Scores test :")
    codes_idx = {c: i for i, c in enumerate(codes_s)}
    for code, label in test.items():
        if code in codes_idx:
            i = codes_idx[code]
            print(f"    {label:<20} : {final_scores[i]:.1f} "
                  f"(trips={commune_trips.get(code,0):,}, gare={scores_gare[i]:.1f})")

    # Construire transport_detail JSON par commune
    TYPE_LABELS = {
        0:  ("Tram",       "🚊"),
        1:  ("Métro",      "🚇"),
        2:  ("RER / TER",  "🚆"),
        3:  ("Bus",        "🚌"),
        4:  ("Ferry",      "⛴"),
        11: ("Trolleybus", "🚎"),
        12: ("Monorail",   "🚝"),
    }
    TYPE_ORDER = [1, 2, 0, 11, 12, 4, 3]  # priorité d'affichage (bus en dernier)
    MAX_BUS  = 10  # max lignes de bus stockées
    MAX_RAIL = 6   # max lignes rail/RER/TER (évite les multiples variantes SNCF)

    def build_transport_detail(routes_set: set) -> str | None:
        if not routes_set:
            return None
        by_type: dict[int, list] = defaultdict(list)
        # Déduplication par (type, short) — élimine aller/retour encodés séparément
        seen: set = set()
        for (rtype, short, long_) in routes_set:
            key = (rtype, short)
            if key in seen:
                continue
            seen.add(key)
            label, icon = TYPE_LABELS.get(rtype, ("Bus", "🚌"))
            by_type[rtype].append({"type_code": rtype, "type_label": label,
                                   "icon": icon, "short": short, "nom": long_})
        lignes = []
        for rtype in TYPE_ORDER:
            if rtype not in by_type:
                continue
            items = sorted(by_type[rtype], key=lambda x: x["short"])
            if rtype == 3:
                items = items[:MAX_BUS]
            elif rtype == 2:
                items = items[:MAX_RAIL]
            lignes.extend(items)
        for rtype, items in by_type.items():
            if rtype not in TYPE_ORDER:
                lignes.extend(sorted(items, key=lambda x: x["short"])[:5])
        if not lignes:
            return None
        return json.dumps({"lignes": lignes}, ensure_ascii=False)

    # Sauvegarde DB
    print("\nSauvegarde en base...")
    async with async_session() as session:
        for i in range(0, len(codes_s), 5000):
            batch_codes = codes_s[i:i+5000]
            for j, code in enumerate(batch_codes):
                td = build_transport_detail(commune_routes.get(code, set()))
                await session.execute(text(
                    "UPDATE scores SET nb_arrets_tc=:n, score_transports=:s, transport_detail=:td WHERE code_insee=:c"
                ), {"n": int(tc_arr[i+j]), "s": float(final_scores[i+j]) if final_scores[i+j] >= 0 else None,
                    "td": td, "c": code})
            await session.commit()
            print(f"  → {min(i+5000, len(codes_s))}/{len(codes_s)}")

    # Scores globaux
    print("\nRecalcul scores globaux...")
    async with async_session() as session:
        res = await session.execute(text("""
            SELECT code_insee, score_equipements, score_securite, score_immobilier,
                   score_education, score_sante, score_revenus, score_transports
            FROM scores WHERE score_transports >= 0 OR score_equipements >= 0
        """))
        global_rows = res.fetchall()
        cols = ["code_insee","score_equipements","score_securite","score_immobilier",
                "score_education","score_sante","score_revenus","score_transports"]
        cat_map = {
            "score_equipements":"equipements","score_securite":"securite",
            "score_immobilier":"immobilier","score_education":"education",
            "score_sante":"sante","score_revenus":"revenus","score_transports":"transports",
        }
        nb = 0
        for row in global_rows:
            r = dict(zip(cols, row))
            ss = {cat: r[col] for col, cat in cat_map.items() if r.get(col) is not None and r[col] >= 0}
            if not ss:
                continue
            score, lettre, n = calculer_score_global(ss)
            await session.execute(text("""
                UPDATE scores SET score_global=:sg, lettre=:l,
                    nb_categories_scorees=:nb, updated_at=CURRENT_TIMESTAMP
                WHERE code_insee=:c
            """), {"sg": score, "l": lettre, "nb": n, "c": r["code_insee"]})
            nb += 1
            if nb % 5000 == 0:
                await session.commit()
        await session.commit()
    print(f"  → {nb} scores globaux recalculés")
    print("\n=== Import GTFS terminé ===")


if __name__ == "__main__":
    asyncio.run(run())
