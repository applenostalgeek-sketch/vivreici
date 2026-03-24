from sqlalchemy import String, Integer, Float, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from backend.database import Base


class IrisZone(Base):
    """Zone IRIS — quartier/groupe de communes (~50 000 zones en France)."""
    __tablename__ = "iris_zones"

    code_iris: Mapped[str] = mapped_column(String(9), primary_key=True)  # 5 commune + 4 iris
    nom: Mapped[str] = mapped_column(String(200))
    code_commune: Mapped[str] = mapped_column(String(5))
    typ_iris: Mapped[str] = mapped_column(String(1), default="H")  # H=habitat, A=activité, D=divers, Z=commune entière
    population: Mapped[int] = mapped_column(Integer, default=0)
    latitude: Mapped[float] = mapped_column(Float, nullable=True)
    longitude: Mapped[float] = mapped_column(Float, nullable=True)
    geometry: Mapped[str] = mapped_column(Text, nullable=True)  # GeoJSON polygon WGS84 simplifié


class IrisScore(Base):
    """Scores de qualité de vie au niveau IRIS."""
    __tablename__ = "iris_scores"

    code_iris: Mapped[str] = mapped_column(String(9), primary_key=True)

    score_global: Mapped[float] = mapped_column(Float, default=0)
    lettre: Mapped[str] = mapped_column(String(1), default="C")

    score_equipements: Mapped[float] = mapped_column(Float, default=-1)
    score_sante: Mapped[float] = mapped_column(Float, default=-1)
    score_immobilier: Mapped[float] = mapped_column(Float, default=-1)
    score_revenus: Mapped[float] = mapped_column(Float, default=-1)

    nb_equipements: Mapped[int] = mapped_column(Integer, default=0)
    nb_medecins_pour_10000: Mapped[float] = mapped_column(Float, default=0)
    prix_m2_median: Mapped[float] = mapped_column(Float, default=0)
    revenu_median: Mapped[float] = mapped_column(Float, default=0)
    taux_pauvrete: Mapped[float] = mapped_column(Float, default=0)

    # Détail des équipements par type (JSON) — ex: {"médecin_généraliste": 2, "pharmacie": 1}
    equipements_detail: Mapped[str] = mapped_column(Text, nullable=True)

    # Présence équipements par type (JSON) — sources GPS : éducation, RES, OSM
    poi_detail: Mapped[str] = mapped_column(Text, nullable=True)

    nb_categories_scorees: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Commune(Base):
    __tablename__ = "communes"

    code_insee: Mapped[str] = mapped_column(String(5), primary_key=True)
    nom: Mapped[str] = mapped_column(String(200))
    departement: Mapped[str] = mapped_column(String(3))
    region: Mapped[str] = mapped_column(String(100), nullable=True)
    population: Mapped[int] = mapped_column(Integer, default=0)
    codes_postaux: Mapped[str] = mapped_column(String(200), default="")  # JSON list as string
    latitude: Mapped[float] = mapped_column(Float, nullable=True)
    longitude: Mapped[float] = mapped_column(Float, nullable=True)
    geometry: Mapped[str] = mapped_column(Text, nullable=True)  # GeoJSON polygon WGS84 simplifié
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Score(Base):
    __tablename__ = "scores"

    code_insee: Mapped[str] = mapped_column(String(5), primary_key=True)

    # Score global (0-100)
    score_global: Mapped[float] = mapped_column(Float, default=0)
    lettre: Mapped[str] = mapped_column(String(1), default="C")

    # Sous-scores (0-100), -1 si donnée manquante
    score_equipements: Mapped[float] = mapped_column(Float, default=-1)
    score_securite: Mapped[float] = mapped_column(Float, default=-1)
    score_immobilier: Mapped[float] = mapped_column(Float, default=-1)
    score_demographie: Mapped[float] = mapped_column(Float, default=-1)
    score_education: Mapped[float] = mapped_column(Float, default=-1)
    score_sante: Mapped[float] = mapped_column(Float, default=-1)
    score_environnement: Mapped[float] = mapped_column(Float, default=-1)

    score_revenus: Mapped[float] = mapped_column(Float, default=-1)   # stocké mais exclu du score global
    score_transports: Mapped[float] = mapped_column(Float, default=-1)

    # Données brutes résumées
    nb_equipements: Mapped[int] = mapped_column(Integer, default=0)
    nb_medecins_pour_10000: Mapped[float] = mapped_column(Float, default=0)
    apl_medecins: Mapped[float] = mapped_column(Float, default=-1)   # APL DREES (consultations/an/hab)
    taux_criminalite: Mapped[float] = mapped_column(Float, default=0)
    prix_m2_median: Mapped[float] = mapped_column(Float, default=0)
    prix_m2_median_2022: Mapped[float] = mapped_column(Float, default=0)  # Pour calcul tendance
    nb_gares: Mapped[int] = mapped_column(Integer, default=0)
    distance_gare_km: Mapped[float] = mapped_column(Float, default=-1)
    evolution_population_5ans: Mapped[float] = mapped_column(Float, default=0)
    revenu_median: Mapped[float] = mapped_column(Float, default=0)
    taux_pauvrete: Mapped[float] = mapped_column(Float, default=0)

    # Détail des équipements par type (JSON) — ex: {"supermarché": 2, "médecin_généraliste": 4}
    equipements_detail: Mapped[str] = mapped_column(Text, nullable=True)

    # Détail POI sources officielles (JSON) — Sirene + FINESS + Annuaire Édu + RES + OSM culture
    # ex: {"boulangerie": 8, "pharmacie": 3, "école_primaire": 4, "piscine": 1, "cinéma": 1}
    poi_detail: Mapped[str] = mapped_column(Text, nullable=True)

    nb_categories_scorees: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
