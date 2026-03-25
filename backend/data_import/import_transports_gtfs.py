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

def parse_zip(zip_bytes: bytes) -> dict:
    """
    Parse un zip GTFS et retourne {(rlat, rlon): nb_trips_uniques}.
    Utilise csv.reader natif — pas de pandas — pour la vitesse.
    Compte tous les trips (pas de filtre calendar) : bonne approximation
    car les ratios entre communes sont conservés.
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
            if not stops_f or not times_f:
                return {}

            # trips.txt → trip_id → route_type (optionnel, on skip)
            # On n'a pas besoin de trips.txt si on ne filtre pas par calendar

            # stop_times.txt → compter les trip_ids uniques par stop_id
            stop_trips: dict[str, set] = defaultdict(set)
            with zf.open(times_f) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                header = next(reader, None)
                if not header:
                    return {}
                header = [h.strip() for h in header]
                try:
                    si = header.index("stop_id")
                    ti = header.index("trip_id")
                except ValueError:
                    return {}
                for row in reader:
                    if len(row) > max(si, ti):
                        stop_trips[row[si]].add(row[ti])

            # stops.txt → lat/lon par stop_id
            result: dict = {}
            with zf.open(stops_f) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                header = next(reader, None)
                if not header:
                    return {}
                header = [h.strip() for h in header]
                try:
                    id_i  = header.index("stop_id")
                    lat_i = header.index("stop_lat")
                    lon_i = header.index("stop_lon")
                except ValueError:
                    return {}
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
                    trips = len(stop_trips.get(sid, set()))
                    if trips == 0:
                        continue
                    pos = (round(lat, 3), round(lon, 3))
                    if trips > result.get(pos, 0):
                        result[pos] = trips
            return result
    except Exception:
        return {}


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

    # Résultat global : (rlat, rlon) → max trips
    global_trips: dict = {}
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
            stops = await loop.run_in_executor(executor, parse_zip, data)
        except Exception:
            stops = {}

        if not stops:
            counters["empty"] += 1
        else:
            for pos, trips in stops.items():
                if trips > global_trips.get(pos, 0):
                    global_trips[pos] = trips
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
    for i, pos in enumerate(positions):
        ci = idxs[i]
        if dists[i] <= max_dists_sphere[ci]:
            c = codes[ci]
            commune_trips[c] = commune_trips.get(c, 0) + global_trips[pos]

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

    # Sauvegarde DB
    print("\nSauvegarde en base...")
    async with async_session() as session:
        for i in range(0, len(codes_s), 5000):
            batch_codes = codes_s[i:i+5000]
            for j, code in enumerate(batch_codes):
                await session.execute(text(
                    "UPDATE scores SET nb_arrets_tc=:n, score_transports=:s WHERE code_insee=:c"
                ), {"n": int(tc_arr[i+j]), "s": float(final_scores[i+j]) if final_scores[i+j] >= 0 else None, "c": code})
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
