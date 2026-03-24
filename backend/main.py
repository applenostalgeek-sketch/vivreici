"""
API FastAPI — VivreIci.fr
Routes principales pour la recherche et le scoring des communes françaises.
"""

import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, func, text
from typing import Optional
import math

from backend.database import init_db, get_db

# Communes parentes PLM — données au niveau arrondissement, ne pas afficher dans classements/cartes
PLM_PARENTS = {"75056", "69123", "13055"}
from backend.models import Commune, Score, IrisZone, IrisScore


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="VivreIci API",
    description="API de scoring de qualité de vie des communes françaises",
    version="0.2.0",
    lifespan=lifespan,
)

import os
_CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:4173").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ─── Helpers ────────────────────────────────────────────────────────────────

def commune_to_dict(commune: Commune) -> dict:
    return {
        "code_insee": commune.code_insee,
        "nom": commune.nom,
        "departement": commune.departement,
        "region": commune.region,
        "population": commune.population,
        "codes_postaux": commune.codes_postaux.split(",") if commune.codes_postaux else [],
        "latitude": commune.latitude,
        "longitude": commune.longitude,
    }


def score_to_dict(score: Score) -> dict:
    return {
        "code_insee": score.code_insee,
        "score_global": score.score_global,
        "lettre": score.lettre,
        "sous_scores": {
            "equipements": score.score_equipements if score.score_equipements >= 0 else None,
            "securite": score.score_securite if score.score_securite >= 0 else None,
            "immobilier": score.score_immobilier if score.score_immobilier >= 0 else None,
            "education": score.score_education if score.score_education >= 0 else None,
            "sante": score.score_sante if score.score_sante >= 0 else None,
            "transports": score.score_transports if score.score_transports >= 0 else None,
            "environnement": score.score_environnement if score.score_environnement >= 0 else None,
            "demographie": score.score_demographie if score.score_demographie >= 0 else None,
        },
        "donnees_brutes": {
            "nb_equipements": score.nb_equipements,
            "apl_medecins": score.apl_medecins if hasattr(score, "apl_medecins") and score.apl_medecins and score.apl_medecins >= 0 else None,
            "taux_criminalite": score.taux_criminalite,
            "prix_m2_median": score.prix_m2_median,
            "prix_m2_median_2022": score.prix_m2_median_2022 if score.prix_m2_median_2022 else None,
            "nb_gares": score.nb_gares if score.nb_gares else 0,
            "distance_gare_km": score.distance_gare_km if score.distance_gare_km and score.distance_gare_km >= 0 else None,
            "equipements_detail": json.loads(score.equipements_detail) if score.equipements_detail else None,
            "poi_detail": json.loads(score.poi_detail) if score.poi_detail else None,
            "evolution_population_5ans": score.evolution_population_5ans,
            "taux_pauvrete": score.taux_pauvrete,
        },
        "nb_categories_scorees": score.nb_categories_scorees,
        "updated_at": score.updated_at.isoformat(),
    }


def iris_lettre(s) -> str | None:
    """Retourne la lettre IRIS si les données sont complètes (>= 2 catégories), sinon None."""
    if s is None:
        return None
    lettre = s.lettre
    return lettre if lettre in ('A', 'B', 'C', 'D', 'E') else None


def iris_score_to_dict(iz: IrisZone, s: IrisScore) -> dict:
    return {
        "code_iris": iz.code_iris,
        "nom": iz.nom,
        "code_commune": iz.code_commune,
        "typ_iris": iz.typ_iris,
        "population": iz.population,
        "latitude": iz.latitude,
        "longitude": iz.longitude,
        "score": {
            "score_global": s.score_global,
            "lettre": iris_lettre(s),
            "sous_scores": {
                "equipements": s.score_equipements if s.score_equipements >= 0 else None,
                "sante": s.score_sante if s.score_sante >= 0 else None,
                "immobilier": s.score_immobilier if s.score_immobilier >= 0 else None,
                "revenus": s.score_revenus if s.score_revenus >= 0 else None,
            },
            "donnees_brutes": {
                "nb_equipements": s.nb_equipements,
                "medecins_pour_10000": s.nb_medecins_pour_10000,
                "prix_m2_median": s.prix_m2_median,
                "revenu_median": s.revenu_median,
                "taux_pauvrete": s.taux_pauvrete,
                "equipements_detail": json.loads(s.equipements_detail) if s.equipements_detail else None,
                "poi_detail": json.loads(s.poi_detail) if s.poi_detail else None,
            },
            "nb_categories_scorees": s.nb_categories_scorees,
        } if s else None,
    }


# ─── Communes ────────────────────────────────────────────────────────────────

@app.get("/api/communes/search")
async def search_communes(
    q: str = Query(..., min_length=2, description="Nom ou code postal"),
    limit: int = Query(10, le=20),
    db: AsyncSession = Depends(get_db),
):
    """Autocomplétion : recherche de communes par nom ou code postal."""
    q_clean = q.strip().lower()

    stmt = (
        select(Commune)
        .where(
            or_(
                func.lower(Commune.nom).like(f"{q_clean}%"),
                Commune.codes_postaux.like(f"%{q_clean}%"),
                Commune.code_insee.like(f"{q_clean}%"),
            )
        )
        .order_by(Commune.population.desc())
        .limit(limit)
    )

    result = await db.execute(stmt)
    communes = result.scalars().all()
    return [commune_to_dict(c) for c in communes]


@app.get("/api/communes/map")
async def communes_map(
    lat_min: float, lat_max: float, lng_min: float, lng_max: float,
    min_population: int = Query(0),
    db: AsyncSession = Depends(get_db),
):
    """Communes dans une bounding box avec géométrie polygon — pour la carte."""
    stmt = (
        select(
            Commune.code_insee, Commune.nom, Commune.population,
            Commune.latitude, Commune.longitude, Commune.geometry,
            Score.score_global, Score.lettre,
        )
        .join(Score, Commune.code_insee == Score.code_insee)
        .where(Commune.latitude >= lat_min)
        .where(Commune.latitude <= lat_max)
        .where(Commune.longitude >= lng_min)
        .where(Commune.longitude <= lng_max)
        .where(Commune.latitude.is_not(None))
        .where(Score.nb_categories_scorees >= 3)
        .where(Commune.code_insee.notin_(PLM_PARENTS))
    )
    if min_population > 0:
        stmt = stmt.where(Commune.population >= min_population)
    stmt = stmt.order_by(Commune.population.desc()).limit(3000)

    result = await db.execute(stmt)
    rows = result.all()
    return [
        {
            "code_insee": r.code_insee, "nom": r.nom, "population": r.population,
            "latitude": r.latitude, "longitude": r.longitude,
            "score_global": r.score_global,
            "lettre": r.lettre if r.lettre in ('A', 'B', 'C', 'D', 'E') else None,
            "geometry": json.loads(r.geometry) if r.geometry else None,
        }
        for r in rows
    ]


@app.get("/api/communes/{code_insee}")
async def get_commune(code_insee: str, db: AsyncSession = Depends(get_db)):
    """Retourne les infos d'une commune + son score + ses IRIS si disponibles."""
    commune = await db.get(Commune, code_insee)
    if not commune:
        raise HTTPException(status_code=404, detail=f"Commune {code_insee} introuvable")

    score = await db.get(Score, code_insee)
    result = commune_to_dict(commune)
    result["score"] = score_to_dict(score) if score else None

    # Rang dans le département
    if commune.departement and score and score.score_global is not None:
        rank_stmt = (
            select(func.count())
            .select_from(Score)
            .join(Commune, Score.code_insee == Commune.code_insee)
            .where(Commune.departement == commune.departement)
            .where(Score.score_global > score.score_global)
            .where(Score.nb_categories_scorees >= 3)
            .where(Commune.code_insee.notin_(PLM_PARENTS))
        )
        total_stmt = (
            select(func.count())
            .select_from(Score)
            .join(Commune, Score.code_insee == Commune.code_insee)
            .where(Commune.departement == commune.departement)
            .where(Score.nb_categories_scorees >= 3)
            .where(Commune.code_insee.notin_(PLM_PARENTS))
        )
        rang = (await db.scalar(rank_stmt) or 0) + 1
        total_dept = await db.scalar(total_stmt) or 0
        result["rang_departement"] = rang
        result["nb_communes_departement"] = total_dept

    # Ajouter les IRIS de cette commune (si disponibles et > 1)
    iris_stmt = (
        select(IrisZone, IrisScore)
        .outerjoin(IrisScore, IrisZone.code_iris == IrisScore.code_iris)
        .where(IrisZone.code_commune == code_insee)
        .where(IrisZone.typ_iris != "Z")  # Exclure IRIS "commune entière"
        # Trier : zones complètes (nb_cat >= 2) d'abord par score, zones partielles à la fin
        .order_by(
            (IrisScore.nb_categories_scorees >= 2).desc().nulls_last(),
            IrisScore.score_global.desc().nulls_last(),
        )
    )
    iris_result = await db.execute(iris_stmt)
    iris_rows = iris_result.all()

    nb_total = len(iris_rows)
    nb_scored = sum(1 for iz, s in iris_rows if iris_lettre(s) is not None)

    if nb_total > 1:
        result["iris"] = [
            {
                "code_iris": iz.code_iris,
                "nom": iz.nom,
                "population": iz.population,
                "latitude": iz.latitude,
                "longitude": iz.longitude,
                "score_global": s.score_global if s else None,
                "lettre": iris_lettre(s),
                "nb_categories_scorees": s.nb_categories_scorees if s else 0,
                "donnees_partielles": (s is not None and (s.nb_categories_scorees or 0) < 2),
                "rang": i + 1,
                "nb_total": nb_total,
                "nb_scored": nb_scored,
            }
            for i, (iz, s) in enumerate(iris_rows)
        ]
    else:
        result["iris"] = []

    return result


@app.get("/api/classement")
async def classement(
    departement: Optional[str] = None,
    region: Optional[str] = None,
    sort: str = Query("score", pattern="^(score|nom|population)$"),
    ordre: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    min_population: int = Query(0, ge=0),
    sante_min: Optional[int] = Query(None, ge=0, le=100),
    securite_min: Optional[int] = Query(None, ge=0, le=100),
    transports_min: Optional[int] = Query(None, ge=0, le=100),
    education_min: Optional[int] = Query(None, ge=0, le=100),
    equipements_min: Optional[int] = Query(None, ge=0, le=100),
    immobilier_min: Optional[int] = Query(None, ge=0, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Classement des communes par score — minimum 3 catégories."""
    stmt = (
        select(Commune, Score)
        .join(Score, Commune.code_insee == Score.code_insee)
        .where(Score.nb_categories_scorees >= 3)
        .where(Commune.code_insee.notin_(PLM_PARENTS))
    )
    if min_population > 0:
        stmt = stmt.where(Commune.population >= min_population)
    if departement:
        stmt = stmt.where(Commune.departement == departement)
    if region:
        stmt = stmt.where(func.lower(Commune.region) == region.lower())
    if sante_min is not None:
        stmt = stmt.where(Score.score_sante >= sante_min)
    if securite_min is not None:
        stmt = stmt.where(Score.score_securite >= securite_min)
    if transports_min is not None:
        stmt = stmt.where(Score.score_transports >= transports_min)
    if education_min is not None:
        stmt = stmt.where(Score.score_education >= education_min)
    if equipements_min is not None:
        stmt = stmt.where(Score.score_equipements >= equipements_min)
    if immobilier_min is not None:
        stmt = stmt.where(Score.score_immobilier >= immobilier_min)

    if sort == "score":
        col = Score.score_global
    elif sort == "population":
        col = Commune.population
    else:
        col = Commune.nom

    stmt = stmt.order_by(col.desc() if ordre == "desc" else col.asc())
    stmt = stmt.offset(offset).limit(limit)

    result = await db.execute(stmt)
    rows = result.all()
    return [{**commune_to_dict(c), "score": score_to_dict(s)} for c, s in rows]


@app.get("/api/compare")
async def compare_communes(c1: str, c2: str, db: AsyncSession = Depends(get_db)):
    """Compare deux communes côte à côte."""
    results = []
    for code in [c1, c2]:
        commune = await db.get(Commune, code)
        if not commune:
            raise HTTPException(status_code=404, detail=f"Commune {code} introuvable")
        score = await db.get(Score, code)
        data = commune_to_dict(commune)
        data["score"] = score_to_dict(score) if score else None
        results.append(data)
    return results


@app.get("/api/map")
async def map_data(
    limit: int = Query(50000, le=50000),
    db: AsyncSession = Depends(get_db),
):
    """Retourne les communes avec scores + coordonnées pour la carte (zoom faible)."""
    stmt = (
        select(
            Commune.code_insee, Commune.nom, Commune.population,
            Commune.latitude, Commune.longitude,
            Score.score_global, Score.lettre,
        )
        .join(Score, Commune.code_insee == Score.code_insee)
        .where(Commune.latitude.is_not(None))
        .where(Commune.longitude.is_not(None))
        .where(Commune.code_insee.notin_(PLM_PARENTS))
        .order_by(Commune.population.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.all()
    return [
        {
            "code_insee": r.code_insee, "nom": r.nom, "population": r.population,
            "latitude": r.latitude, "longitude": r.longitude,
            "score_global": r.score_global, "lettre": r.lettre,
        }
        for r in rows
    ]


# ─── IRIS ────────────────────────────────────────────────────────────────────

PLM_PREFIXES = {"75056": "751", "69123": "6938", "13055": "132"}


@app.get("/api/communes/{code_insee}/iris")
async def get_commune_iris_list(code_insee: str, db: AsyncSession = Depends(get_db)):
    """Tous les IRIS d'une commune avec sous-scores complets — pour la page quartiers.
    Gère les communes PLM (Paris/Lyon/Marseille) en agrégeant leurs arrondissements."""

    if code_insee in PLM_PREFIXES:
        prefix = PLM_PREFIXES[code_insee]
        where_clause = IrisZone.code_commune.like(f"{prefix}%")
    else:
        where_clause = (IrisZone.code_commune == code_insee)

    stmt = (
        select(IrisZone, IrisScore)
        .outerjoin(IrisScore, IrisZone.code_iris == IrisScore.code_iris)
        .where(where_clause)
        .where(IrisZone.typ_iris != "Z")
        # Zones complètes (nb_cat >= 2) d'abord, partielles à la fin
        .order_by(
            (IrisScore.nb_categories_scorees >= 2).desc().nulls_last(),
            IrisScore.score_global.desc().nulls_last(),
        )
    )
    result = await db.execute(stmt)
    rows = result.all()

    # Rang basé uniquement sur les zones complètes (nb_cat >= 2)
    complete_iris = [(iz, s) for iz, s in rows if s and (s.nb_categories_scorees or 0) >= 2]
    complete_codes = [iz.code_iris for iz, s in complete_iris]

    output = []
    rang_counter = 0
    for iz, s in rows:
        est_partiel = s is None or (s.nb_categories_scorees or 0) < 2
        if not est_partiel:
            rang_counter += 1
            rang = rang_counter
        else:
            rang = None
        output.append({
            "code_iris":   iz.code_iris,
            "nom":         iz.nom,
            "code_commune": iz.code_commune,
            "typ_iris":    iz.typ_iris,
            "population":  iz.population,
            "rang":        rang,
            "nb_total":    len(complete_iris),  # total des zones comparables
            "score_global": s.score_global if s else None,
            "lettre":      iris_lettre(s),
            "nb_categories_scorees": s.nb_categories_scorees if s else 0,
            "donnees_partielles": est_partiel,
            "sous_scores": {
                "equipements": s.score_equipements if s and s.score_equipements >= 0 else None,
                "sante":       s.score_sante       if s and s.score_sante       >= 0 else None,
                "immobilier":  s.score_immobilier  if s and s.score_immobilier  >= 0 else None,
                "revenus":     s.score_revenus     if s and s.score_revenus     >= 0 else None,
            } if s else {},
        })
    return output


@app.get("/api/iris/compare")
async def compare_iris_zones(c1: str, c2: str, db: AsyncSession = Depends(get_db)):
    """Compare deux zones IRIS côte à côte."""
    results = []
    for code in [c1, c2]:
        iz = await db.get(IrisZone, code)
        if not iz:
            raise HTTPException(status_code=404, detail=f"IRIS {code} introuvable")
        s = await db.get(IrisScore, code)
        data = iris_score_to_dict(iz, s)
        data["code_commune"] = iz.code_commune
        # Add rank within commune
        rank_stmt = (
            select(IrisScore.code_iris)
            .join(IrisZone, IrisZone.code_iris == IrisScore.code_iris)
            .where(IrisZone.code_commune == iz.code_commune)
            .where(IrisZone.typ_iris != "Z")
            .where(IrisScore.nb_categories_scorees >= 2)
            .order_by(IrisScore.score_global.desc())
        )
        rank_result = await db.execute(rank_stmt)
        all_codes = [r[0] for r in rank_result.all()]
        try:
            data["rang_commune"] = all_codes.index(code) + 1
        except ValueError:
            data["rang_commune"] = None
        data["nb_iris_commune"] = len(all_codes)
        results.append(data)
    return results


@app.get("/api/iris/map")
async def iris_map(
    lat_min: float, lat_max: float, lng_min: float, lng_max: float,
    db: AsyncSession = Depends(get_db),
):
    """IRIS dans une bounding box — pour la carte zoomée (zoom >= 11).
    - IRIS non-Z (quartiers réels) : colorés avec leur score IRIS
    - IRIS type-Z (commune entière) : colorés avec le score de la commune parente
    """
    bbox_where = [
        IrisZone.latitude >= lat_min, IrisZone.latitude <= lat_max,
        IrisZone.longitude >= lng_min, IrisZone.longitude <= lng_max,
        IrisZone.latitude.is_not(None),
    ]

    # 1. Quartiers réels (H/A/D) avec score IRIS suffisant
    stmt_iris = (
        select(
            IrisZone.code_iris, IrisZone.nom, IrisZone.code_commune,
            IrisZone.population, IrisZone.latitude, IrisZone.longitude,
            IrisZone.typ_iris, IrisZone.geometry,
            IrisScore.score_global, IrisScore.lettre,
        )
        .join(IrisScore, IrisZone.code_iris == IrisScore.code_iris)
        .where(*bbox_where)
        .where(IrisZone.typ_iris != "Z")
        .where(IrisScore.nb_categories_scorees >= 2)
        .limit(2000)
    )
    res_iris = await db.execute(stmt_iris)
    rows_iris = res_iris.all()

    # 2. IRIS type-Z (petites communes = commune entière) → score de la commune parente
    stmt_z = (
        select(
            IrisZone.code_iris, IrisZone.nom, IrisZone.code_commune,
            IrisZone.population, IrisZone.latitude, IrisZone.longitude,
            IrisZone.typ_iris, IrisZone.geometry,
            Score.score_global, Score.lettre,
        )
        .join(Score, IrisZone.code_commune == Score.code_insee)
        .where(*bbox_where)
        .where(IrisZone.typ_iris == "Z")
        .where(Score.nb_categories_scorees >= 3)
        .limit(3000)
    )
    res_z = await db.execute(stmt_z)
    rows_z = res_z.all()

    def row_to_dict(r):
        return {
            "code_iris": r.code_iris, "nom": r.nom,
            "code_commune": r.code_commune,
            "population": r.population,
            "latitude": r.latitude, "longitude": r.longitude,
            "typ_iris": r.typ_iris,
            "score_global": r.score_global,
            "lettre": r.lettre if r.lettre in ('A', 'B', 'C', 'D', 'E') else None,
            "geometry": json.loads(r.geometry) if r.geometry else None,
        }

    return [row_to_dict(r) for r in rows_iris] + [row_to_dict(r) for r in rows_z]


@app.get("/api/iris/{code_iris}")
async def get_iris(code_iris: str, db: AsyncSession = Depends(get_db)):
    """Détail d'une zone IRIS."""
    iz = await db.get(IrisZone, code_iris)
    if not iz:
        raise HTTPException(status_code=404, detail=f"IRIS {code_iris} introuvable")
    s = await db.get(IrisScore, code_iris)
    result = iris_score_to_dict(iz, s)

    # Rang de cet IRIS parmi les zones complètes (nb_cat >= 2) de la même commune
    all_stmt = (
        select(IrisScore.code_iris)
        .join(IrisZone, IrisZone.code_iris == IrisScore.code_iris)
        .where(IrisZone.code_commune == iz.code_commune)
        .where(IrisZone.typ_iris != "Z")
        .where(IrisScore.nb_categories_scorees >= 2)
        .order_by(IrisScore.score_global.desc())
    )
    all_result = await db.execute(all_stmt)
    all_iris_codes = [row[0] for row in all_result.all()]

    nb_iris_commune = len(all_iris_codes)
    rang_commune = None
    if nb_iris_commune > 0:
        try:
            rang_commune = all_iris_codes.index(code_iris) + 1
        except ValueError:
            rang_commune = None

    result["rang_commune"] = rang_commune
    result["nb_iris_commune"] = nb_iris_commune
    result["code_commune"] = iz.code_commune

    # Nom de la commune pour le breadcrumb (fallback sur code si non trouvée)
    commune = await db.get(Commune, iz.code_commune)
    result["commune_nom"] = commune.nom if commune else iz.code_commune

    return result


@app.get("/api/recherche-geo")
async def recherche_geo(
    lat: float,
    lng: float,
    rayon_km: float = Query(20.0, ge=1, le=100),
    score_min: float = Query(0.0, ge=0, le=100),
    min_population: int = Query(0, ge=0),
    limit: int = Query(100, le=300),
    db: AsyncSession = Depends(get_db),
):
    """Communes dans un rayon géographique avec filtre de score minimum.
    Utilise une bounding box SQL + haversine exact en post-filtrage.
    """
    # Bounding box approximative (1° lat ≈ 111 km)
    lat_delta = rayon_km / 111.0
    lng_delta = rayon_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))

    stmt = (
        select(Commune, Score)
        .join(Score, Commune.code_insee == Score.code_insee)
        .where(Commune.latitude.between(lat - lat_delta, lat + lat_delta))
        .where(Commune.longitude.between(lng - lng_delta, lng + lng_delta))
        .where(Score.score_global >= score_min)
        .where(Score.nb_categories_scorees >= 3)
        .where(Commune.latitude.is_not(None))
        .where(Commune.longitude.is_not(None))
    )
    if min_population > 0:
        stmt = stmt.where(Commune.population >= min_population)

    result = await db.execute(stmt)
    rows = result.all()

    def haversine(lat1, lng1, lat2, lng2):
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlng = math.radians(lng2 - lng1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
        return R * 2 * math.asin(math.sqrt(a))

    results = []
    for c, s in rows:
        dist = haversine(lat, lng, c.latitude, c.longitude)
        if dist <= rayon_km:
            results.append({
                **commune_to_dict(c),
                "score": score_to_dict(s),
                "distance_km": round(dist, 1),
            })

    results.sort(key=lambda x: x["distance_km"])
    return results[:limit]


@app.get("/api/locate")
async def locate(lat: float, lng: float, db: AsyncSession = Depends(get_db)):
    """Trouve l'IRIS le plus proche d'un point GPS — pour la recherche par adresse."""
    # Bounding box ~15 km pour limiter les candidats
    result = await db.execute(text("""
        SELECT code_iris, nom, code_commune,
               ((latitude - :lat)*(latitude - :lat) + (longitude - :lng)*(longitude - :lng)) as dist2
        FROM iris_zones
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY dist2
        LIMIT 1
    """), {
        "lat": lat, "lng": lng,
        "lat_min": lat - 0.15, "lat_max": lat + 0.15,
        "lng_min": lng - 0.15, "lng_max": lng + 0.15,
    })
    row = result.fetchone()
    if not row:
        return {"code_iris": None, "code_commune": None}
    return {"code_iris": row[0], "nom": row[1], "code_commune": row[2]}


@app.get("/api/stats")
async def stats(db: AsyncSession = Depends(get_db)):
    """Statistiques globales."""
    nb_communes = await db.scalar(select(func.count()).select_from(Commune))
    nb_scorees = await db.scalar(select(func.count()).select_from(Score))
    nb_iris = await db.scalar(select(func.count()).select_from(IrisZone))

    return {
        "nb_communes": nb_communes or 0,
        "nb_scorees": nb_scorees or 0,
        "nb_iris": nb_iris or 0,
        "categories": ["equipements", "sante", "securite", "immobilier", "education",
                       "transports", "environnement", "demographie"],
    }


# ─── Servir le frontend React en production ────────────────────────────────────
# En dev, Vite tourne sur :5173 séparément.
# En prod, le build React est dans frontend/dist/ — FastAPI le sert directement.

import pathlib
_FRONTEND_DIST = pathlib.Path(__file__).parent.parent / "frontend" / "dist"

if _FRONTEND_DIST.exists():
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse

    app.mount("/assets", StaticFiles(directory=_FRONTEND_DIST / "assets"), name="assets")

    # Route catch-all : toutes les URLs non-API → index.html (SPA routing)
    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        index = _FRONTEND_DIST / "index.html"
        if index.exists():
            return FileResponse(index)
        return {"error": "Frontend non compilé — lancer npm run build dans frontend/"}
