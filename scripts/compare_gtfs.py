"""
Comparaison des 4 scénarios de score transport pour des communes test.

Scénario A — Score actuel (DB)   : 50% gare + 50% nb_arrets_tc (filtre ≥3 occurrences + Voronoi)
Scénario B — Gare seule           : 100% distance gare SNCF
Scénario C — GTFS trips/jour      : 50% gare + 50% trips TC par jour ouvré (IDFM + Centre-VdL)
Scénario D — Somme occurrences    : 50% gare + 50% somme des occurrences par arrêt (CSV national)

Télécharge :
- CSV arrêts national (416 Mo)    → scénario D
- IDFM GTFS (123 Mo)              → scénario C, couvre Paris + Rambouillet
- Centre-Val-de-Loire GTFS (36 Mo)→ scénario C, couvre Eure-et-Loir (Bouglainval)

Usage : python3 scripts/compare_gtfs.py
"""

import asyncio
import io
import os
import sys
import tempfile
import zipfile
from collections import Counter, defaultdict

import httpx
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from backend.database import async_session, init_db
from backend.scoring import percentile_to_score
from sqlalchemy import text

# ── Communes test ──────────────────────────────────────────────────────────────
TEST_COMMUNES = {
    "28052": "Bouglainval",
    "78517": "Rambouillet",
    "75101": "Paris 1er",
    "75110": "Paris 10e",
    "75115": "Paris 15e",
    "75118": "Paris 18e",
}

# ── Sources ────────────────────────────────────────────────────────────────────
STOPS_CSV_URL = (
    "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com"
    "/81333/81333.20260113.121610.549772.csv"
)
GTFS_FEEDS = [
    ("IDFM",       "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"),
    ("Centre-VdL", "https://www.data.gouv.fr/api/1/datasets/r/a193c142-366b-4a7b-8afb-c0bdf23ca7ea"),
]

LAT_MIN, LAT_MAX = 41.0, 52.0
LON_MIN, LON_MAX = -6.0, 10.0
CHUNK_SIZE = 300_000
MIN_OCCURRENCES = 3


# ── KDTree + Voronoi (partagé par tous les scénarios) ─────────────────────────
def build_tree(comm_lats, comm_lons):
    R = 1.0
    xs = R * np.cos(np.radians(comm_lats)) * np.cos(np.radians(comm_lons))
    ys = R * np.cos(np.radians(comm_lats)) * np.sin(np.radians(comm_lons))
    zs = R * np.sin(np.radians(comm_lats))
    coords = np.column_stack([xs, ys, zs])
    tree = cKDTree(coords)
    self_dists, _ = tree.query(coords, k=2)
    nn_dists_km = self_dists[:, 1] * 6371.0
    max_dists_sphere = np.clip(nn_dists_km * 0.5, 1.0, 5.0) / 6371.0
    return tree, coords, max_dists_sphere


def query_points(tree, max_dists_sphere, rlats, rlons):
    """Retourne (dists, idxs) pour un tableau de positions lat/lon."""
    R = 1.0
    rlat_r = np.radians(rlats)
    rlon_r = np.radians(rlons)
    xs_q = R * np.cos(rlat_r) * np.cos(rlon_r)
    ys_q = R * np.cos(rlat_r) * np.sin(rlon_r)
    zs_q = R * np.sin(rlat_r)
    dists, idxs = tree.query(np.column_stack([xs_q, ys_q, zs_q]))
    return dists, idxs


# ── Scénario D : somme des occurrences (CSV national) ─────────────────────────
async def compute_occurrence_sum(codes, comm_lats, comm_lons, tree, max_dists_sphere):
    """
    Télécharge le CSV national des arrêts.
    Passe 1 : compte les occurrences par position (rlat, rlon).
    Passe 2 : pour chaque position qualifiée (≥ MIN_OCCURRENCES),
              ajoute SON NOMBRE D'OCCURRENCES à la commune assignée.

    Résultat : {code_insee: somme_occurrences} — proxy d'intensité de service.
    Un arrêt de métro (40 occurrences) pèse bien plus qu'un car rural (3 occurrences).
    """
    print("\n--- Scénario D : somme occurrences (CSV national) ---")

    # Téléchargement
    print("  Téléchargement CSV arrêts (~416 Mo)...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=600) as client:
        async with client.stream("GET", STOPS_CSV_URL) as resp:
            resp.raise_for_status()
            total = 0
            tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, prefix="tc_occ_")
            tmp_path = tmp.name
            tmp.close()
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=4 * 1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
                    if total % (100 * 1024 * 1024) == 0:
                        print(f"  → {total // (1024*1024)} Mo...")
    print(f"  → Téléchargé ({total // (1024*1024)} Mo)")

    csv_params = dict(
        chunksize=CHUNK_SIZE,
        usecols=["stop_lat", "stop_lon"],
        dtype={"stop_lat": float, "stop_lon": float},
        on_bad_lines="skip",
        low_memory=False,
    )

    # Passe 1 : compter les occurrences par position
    print("  Passe 1/2 : comptage occurrences...")
    position_counts: Counter = Counter()
    total_lignes = 0
    for chunk in pd.read_csv(tmp_path, **csv_params):
        total_lignes += len(chunk)
        chunk = chunk[
            (chunk["stop_lat"] >= LAT_MIN) & (chunk["stop_lat"] <= LAT_MAX) &
            (chunk["stop_lon"] >= LON_MIN) & (chunk["stop_lon"] <= LON_MAX)
        ].copy()
        if chunk.empty:
            continue
        chunk["rlat"] = chunk["stop_lat"].round(3)
        chunk["rlon"] = chunk["stop_lon"].round(3)
        for (rlat, rlon), cnt in chunk.groupby(["rlat", "rlon"]).size().items():
            position_counts[(rlat, rlon)] += cnt

    qualifying = {pos: cnt for pos, cnt in position_counts.items() if cnt >= MIN_OCCURRENCES}
    print(f"  → {len(qualifying):,} positions qualifiées (≥{MIN_OCCURRENCES})")

    # Montrer la distribution des occurrences pour les positions qualifiées
    occ_vals = np.array(list(qualifying.values()))
    print(f"  → Distribution occurrences (qualifiées) : "
          f"médiane={np.median(occ_vals):.0f}, p75={np.percentile(occ_vals,75):.0f}, "
          f"p90={np.percentile(occ_vals,90):.0f}, max={occ_vals.max()}")

    # Passe 2 : assigner à chaque commune la SOMME des occurrences
    print("  Passe 2/2 : assignation...")
    qualifying_df = pd.DataFrame(
        [(rlat, rlon, cnt) for (rlat, rlon), cnt in qualifying.items()],
        columns=["rlat", "rlon", "occ"]
    )

    commune_occ_sum = defaultdict(float)
    commune_stop_count = defaultdict(int)

    for chunk in pd.read_csv(tmp_path, **csv_params):
        chunk = chunk[
            (chunk["stop_lat"] >= LAT_MIN) & (chunk["stop_lat"] <= LAT_MAX) &
            (chunk["stop_lon"] >= LON_MIN) & (chunk["stop_lon"] <= LON_MAX)
        ].copy()
        if chunk.empty:
            continue
        chunk["rlat"] = chunk["stop_lat"].round(3)
        chunk["rlon"] = chunk["stop_lon"].round(3)
        chunk = chunk.drop_duplicates(subset=["rlat", "rlon"])
        chunk = chunk.merge(qualifying_df, on=["rlat", "rlon"], how="inner")
        if chunk.empty:
            continue

        dists, idxs = query_points(tree, max_dists_sphere,
                                    chunk["rlat"].values, chunk["rlon"].values)
        for i in range(len(chunk)):
            comm_idx = idxs[i]
            if dists[i] <= max_dists_sphere[comm_idx]:
                code = codes[comm_idx]
                commune_occ_sum[code] += chunk.iloc[i]["occ"]
                commune_stop_count[code] += 1

    os.unlink(tmp_path)
    print(f"  → {len(commune_occ_sum):,} communes avec somme occurrences > 0")

    # Afficher le détail pour les communes test
    print("\n  Détail communes test (Scénario D) :")
    for code, label in TEST_COMMUNES.items():
        occ = commune_occ_sum.get(code, 0)
        nb = commune_stop_count.get(code, 0)
        avg = occ / nb if nb > 0 else 0
        print(f"    {label:<20} : {nb} arrêts, somme_occ={occ:.0f}, moy_occ/arrêt={avg:.1f}")

    return dict(commune_occ_sum)


# ── Scénario C : GTFS trips/jour ──────────────────────────────────────────────
async def download_gtfs(name, url):
    print(f"  Téléchargement {name}...")
    async with httpx.AsyncClient(follow_redirects=True, timeout=300) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        print(f"  → {name} : {len(resp.content) // (1024*1024)} Mo")
        return resp.content


def parse_gtfs_trips_per_stop(zip_bytes, feed_name):
    print(f"  Parsing {feed_name}...")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        files = zf.namelist()
        weekday_services = set()
        if "calendar.txt" in files:
            df_cal = pd.read_csv(zf.open("calendar.txt"), dtype=str, on_bad_lines="skip")
            df_cal.columns = df_cal.columns.str.strip()
            wd_cols = [c for c in ["monday","tuesday","wednesday","thursday","friday"] if c in df_cal.columns]
            for col in wd_cols:
                df_cal[col] = pd.to_numeric(df_cal[col], errors="coerce").fillna(0)
            if wd_cols and "service_id" in df_cal.columns:
                weekday_services = set(df_cal.loc[df_cal[wd_cols].sum(axis=1) >= 3, "service_id"].astype(str))

        if not weekday_services and "calendar_dates.txt" in files:
            df_cd = pd.read_csv(zf.open("calendar_dates.txt"), dtype=str, on_bad_lines="skip")
            df_cd.columns = df_cd.columns.str.strip()
            if "service_id" in df_cd.columns:
                weekday_services = set(df_cd["service_id"].astype(str).unique())

        trip_to_service = {}
        if "trips.txt" in files:
            df_trips = pd.read_csv(zf.open("trips.txt"), dtype=str, on_bad_lines="skip",
                                   usecols=lambda c: c in ["trip_id", "service_id"])
            df_trips.columns = df_trips.columns.str.strip()
            if "trip_id" in df_trips.columns and "service_id" in df_trips.columns:
                trip_to_service = dict(zip(df_trips["trip_id"].astype(str),
                                           df_trips["service_id"].astype(str)))

        stop_trip_counts = defaultdict(set)
        if "stop_times.txt" in files:
            for chunk in pd.read_csv(zf.open("stop_times.txt"), dtype=str,
                                     usecols=lambda c: c in ["stop_id", "trip_id"],
                                     chunksize=500_000, on_bad_lines="skip"):
                chunk.columns = chunk.columns.str.strip()
                if "stop_id" not in chunk.columns or "trip_id" not in chunk.columns:
                    continue
                chunk = chunk.dropna(subset=["stop_id", "trip_id"])
                chunk["service_id"] = chunk["trip_id"].astype(str).map(trip_to_service)
                if weekday_services:
                    chunk = chunk[chunk["service_id"].isin(weekday_services)]
                for stop_id, trip_id in zip(chunk["stop_id"].astype(str), chunk["trip_id"].astype(str)):
                    stop_trip_counts[stop_id].add(trip_id)

        df_stops = pd.read_csv(zf.open("stops.txt"), dtype=str, on_bad_lines="skip",
                               usecols=lambda c: c in ["stop_id", "stop_lat", "stop_lon"])
        df_stops.columns = df_stops.columns.str.strip()
        df_stops["stop_lat"] = pd.to_numeric(df_stops["stop_lat"], errors="coerce")
        df_stops["stop_lon"] = pd.to_numeric(df_stops["stop_lon"], errors="coerce")
        df_stops = df_stops.dropna(subset=["stop_lat", "stop_lon"])
        df_stops = df_stops[
            (df_stops["stop_lat"] >= LAT_MIN) & (df_stops["stop_lat"] <= LAT_MAX) &
            (df_stops["stop_lon"] >= LON_MIN) & (df_stops["stop_lon"] <= LON_MAX)
        ]
        df_stops["rlat"] = df_stops["stop_lat"].round(3)
        df_stops["rlon"] = df_stops["stop_lon"].round(3)

    result = {}
    for _, row in df_stops.iterrows():
        trips = len(stop_trip_counts.get(str(row["stop_id"]), set()))
        if trips == 0:
            continue
        pos = (row["rlat"], row["rlon"])
        result[pos] = max(result.get(pos, 0), trips)
    print(f"  → {len(result):,} positions avec trips > 0")
    return result


def assign_gtfs_to_communes(stops_trips, codes, tree, max_dists_sphere):
    result = defaultdict(lambda: {"nb_stops": 0, "total_trips": 0})
    if not stops_trips:
        return result
    positions = list(stops_trips.keys())
    rlats = np.array([p[0] for p in positions])
    rlons = np.array([p[1] for p in positions])
    dists, idxs = query_points(tree, max_dists_sphere, rlats, rlons)
    for i, pos in enumerate(positions):
        comm_idx = idxs[i]
        if dists[i] <= max_dists_sphere[comm_idx]:
            code = codes[comm_idx]
            result[code]["nb_stops"] += 1
            result[code]["total_trips"] += stops_trips[pos]
    return result


# ── Main ───────────────────────────────────────────────────────────────────────
async def run():
    print("=== Comparaison 4 scénarios transport ===\n")
    await init_db()

    # Charger les données DB
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT c.code_insee, c.nom, c.population, c.latitude, c.longitude,
                   s.nb_arrets_tc, s.distance_gare_km, s.score_transports
            FROM communes c JOIN scores s ON s.code_insee = c.code_insee
            WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL
        """))
        rows = result.fetchall()

    codes     = [r[0] for r in rows]
    comm_lats = np.array([float(r[3]) for r in rows])
    comm_lons = np.array([float(r[4]) for r in rows])
    meta      = {r[0]: {"nom": r[1], "pop": r[2], "nb_tc": r[5] or 0,
                         "dist": float(r[6]) if r[6] is not None else None,
                         "score_a": float(r[7]) if r[7] is not None else None}
                 for r in rows}
    print(f"  {len(codes)} communes chargées")

    # Construire KDTree + Voronoi (partagé)
    tree, _, max_dists_sphere = build_tree(comm_lats, comm_lons)

    # Scénario D : somme occurrences (CSV)
    occ_sums = await compute_occurrence_sum(codes, comm_lats, comm_lons, tree, max_dists_sphere)

    # Scénario C : GTFS trips
    print("\n--- Scénario C : GTFS trips/jour ---")
    all_stops_trips = {}
    for name, url in GTFS_FEEDS:
        zip_bytes = await download_gtfs(name, url)
        feed_stops = parse_gtfs_trips_per_stop(zip_bytes, name)
        for pos, trips in feed_stops.items():
            all_stops_trips[pos] = max(all_stops_trips.get(pos, 0), trips)
    gtfs_data = assign_gtfs_to_communes(all_stops_trips, codes, tree, max_dists_sphere)
    print(f"  → {sum(1 for v in gtfs_data.values() if v['nb_stops'] > 0)} communes avec GTFS")

    # Distributions nationales pour les percentiles
    dists_valides  = [m["dist"] for m in meta.values() if m["dist"] is not None]
    serie_dist     = pd.Series(dists_valides)
    serie_occ      = pd.Series([occ_sums.get(c, 0.0) for c in codes], dtype=float)
    serie_trips    = pd.Series([gtfs_data.get(c, {}).get("total_trips", 0) for c in codes], dtype=float)

    # ── Tableau de résultats ───────────────────────────────────────────────────
    print("\n" + "="*120)
    print(f"  {'Commune':<20} {'Pop':>8}  {'nb_TC':>5}  {'dist':>6}  "
          f"{'occ_sum':>8}  {'trips/j':>7}  "
          f"{'Score A':>8}  {'Score B':>8}  {'Score C':>8}  {'Score D':>8}")
    print(f"  {'':20} {'':8}  {'actuel':>5}  {'gare':>6}  "
          f"{'ΣOcc':>8}  {'GTFS':>7}  "
          f"{'Actuel':>8}  {'Gare seul':>8}  {'GTFS':>8}  {'ΣOcc':>8}")
    print("  " + "-"*118)

    for code, label in TEST_COMMUNES.items():
        if code not in meta:
            print(f"  {label}: NON TROUVÉ")
            continue
        m = meta[code]
        occ  = occ_sums.get(code, 0)
        g    = gtfs_data.get(code, {"nb_stops": 0, "total_trips": 0})

        s_a = m["score_a"]
        s_b = percentile_to_score(m["dist"], serie_dist, "inverse") if m["dist"] is not None else None
        s_c_trips = percentile_to_score(float(g["total_trips"]), serie_trips, "direct")
        s_c = round(0.5 * s_b + 0.5 * s_c_trips, 1) if s_b is not None else s_c_trips
        s_d_occ = percentile_to_score(float(occ), serie_occ, "direct")
        s_d = round(0.5 * s_b + 0.5 * s_d_occ, 1) if s_b is not None else s_d_occ

        def f(v): return f"{v:.1f}" if v is not None else " N/A"
        dist_s = f"{m['dist']:.1f}" if m["dist"] is not None else "N/A"

        print(f"  {label:<20} {m['pop']:>8,}  {m['nb_tc']:>5}  {dist_s:>5}km  "
              f"{occ:>8,.0f}  {g['total_trips']:>7,}  "
              f"{f(s_a):>8}  {f(s_b):>8}  {f(s_c):>8}  {f(s_d):>8}")

    print("  " + "="*118)
    print("""
  Score A  Actuel    : 50% gare + 50% nb_arrets_tc (filtre ≥3 occurrences + Voronoi)
  Score B  Gare seul : 100% distance gare SNCF
  Score C  GTFS      : 50% gare + 50% trips/jour (IDFM + Centre-VdL, distribution PARTIELLE)
  Score D  ΣOcc      : 50% gare + 50% somme des occurrences par arrêt (CSV national complet)

  Note Score C : distribution partielle (~2K communes sur 35K) → percentiles biaisés.
  Note Score D : distribution nationale complète (35K communes) → percentiles fiables.
""")


if __name__ == "__main__":
    asyncio.run(run())
