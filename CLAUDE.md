# VivreIci — Instructions de développement

## Contexte
Plateforme de scoring de qualité de vie des communes françaises basée sur l'open data.
Score A-E par commune (style Nutri-Score) — 9 catégories.

## Conventions
- Python 3.11+, FastAPI, SQLAlchemy async (aiosqlite)
- React 18+, Vite, Tailwind CSS
- Commentaires en français
- Noms de variables/fonctions en anglais (snake_case Python, camelCase JS)
- Toujours utiliser le code INSEE (5 chiffres) comme clé primaire pour les communes
- CRITICAL: Toujours lancer uvicorn depuis `/Users/admin/vivreici/` (chemin DB relatif)

## Commandes fréquentes

### Serveurs
- Backend : `cd /Users/admin/vivreici && uvicorn backend.main:app --reload --port 8082`
- Frontend : `cd /Users/admin/vivreici/frontend && npm run dev`

### Imports de données (ordre recommandé)
Depuis `/Users/admin/vivreici/` :
1. `python -m backend.data_import.import_geo`         → communes (API Géo)
2. `python -m backend.data_import.import_geo_fallback`→ communes manquantes (GeoJSON)
3. `python -m backend.data_import.import_coords`      → coordonnées GPS
4. `python -m backend.data_import.import_bpe`         → équipements + santé (BPE 2024 INSEE, ~5min)
5. `python -m backend.data_import.import_securite`    → criminalité (SSMSI 2024)
6. `python -m backend.data_import.import_education`   → IPS collèges + DNB + lycées pro
7. `python -m backend.data_import.import_dvf`         → immobilier (DVF 2024, ~3min)
8. `python -m backend.data_import.import_filosofi`    → revenus/pauvreté (Filosofi 2021)
9. `python3 -m backend.data_import.import_transports`  → score transports (gares SNCF, ~1min)
10. `python3 -m backend.data_import.import_demographie` → évolution population 2016→2021
11. `python3 -m backend.data_import.import_environnement` → artificialisation sols (data.gouv.fr)
12. `python3 -m backend.data_import.import_apl`         → santé APL DREES 2023 (remplace BPE medecins)

### Import POI — présence équipements par commune ET par IRIS (sources officielles)
Depuis `/Users/admin/vivreici/` (ordre recommandé, chaque script est indépendant) :
1. `python3 -m backend.data_import.import_finess`         → pharmacies, hôpitaux, cliniques (FINESS data.gouv.fr) — commune uniquement
2. `python3 -m backend.data_import.import_education_poi`  → écoles, collèges, lycées (Annuaire éducation) — commune
3. `python3 -m backend.data_import.import_res`            → piscines, gymnases, stades (RES data.sports.gouv.fr) — commune
4. `python3 -m backend.data_import.import_culture_osm`    → cinémas, boulangeries, supermarchés (OSM Overpass, ~5 min) — commune + IRIS
5. `python3 -m backend.data_import.import_poi_iris`       → éducation + sports par IRIS (GPS matching, ~10 min) — IRIS uniquement

Notes :
- Affichage présence uniquement (pas de counts) — données stockées JSON avec valeur = nombre réel mais affichées en tags
- Rollback : retirer le bloc poi_detail dans Commune.jsx + Iris.jsx + champ poi_detail dans main.py
- Sirene (commerce) abandonné — fichier 2.6 Go, remplacé par OSM (bakery, supermarket, butcher)

Données stockées dans `scores.poi_detail` (JSON). Totalement additif — rollback = retirer le bloc d'affichage dans Commune.jsx + poi_detail dans main.py.

### Import minimal (si BPE déjà fait)
Lancer sécu + edu + dvf + filosofi + transports sans BPE.

### Import historique DVF (optionnel — pour la tendance des prix)
Depuis `/Users/admin/vivreici/` :
- `python3 -m backend.data_import.import_dvf_historique`  → DVF 2022, ~3min — stocke prix_m2_median_2022

### Imports IRIS (après les imports communes)
Depuis `/Users/admin/vivreici/` :
1. `python3 -m backend.data_import.import_iris_zones`       → zones IRIS + centroides (~50 000 zones, ~5min)
2. `python3 -m backend.data_import.import_bpe_iris`         → équipements + santé par IRIS (~3min)
3. `python3 -m backend.data_import.import_filosofi_iris`    → revenus par IRIS (~2min)
4. `python3 -m backend.data_import.import_dvf_iris`         → immobilier par IRIS (jointure spatiale, ~15min)
5. `python3 -m backend.data_import.import_iris_geometry`    → contours polygons IRIS pour la carte (~5min, optionnel)

## Architecture

### Backend DB (SQLite vivreici.db)
- `communes` : code_insee, nom, departement, region, population, codes_postaux, latitude, longitude
- `scores` : 8 sous-scores (-1 si absent), données brutes, score_global, lettre, nb_categories_scorees
- `iris_zones` : code_iris (9 chars = commune 5 + iris 4), nom, code_commune, typ_iris, population, latitude, longitude
- `iris_scores` : 4 sous-scores IRIS (equipements, sante, immobilier, revenus), score_global, lettre

### Scores (0-100, percentile national)
- `score_equipements` : BPE 2024 (équipements pour 1000 hab)
- `score_sante` : BPE 2024 (médecins pour 10000 hab)
- `score_securite` : SSMSI 2024 (taux criminalité, sens inverse)
- `score_immobilier` : DVF 2024 (prix m² médian, sens inverse)
- `score_education` : IPS collèges 2024-2025 (40%) + DNB brevet 2021 (40%) + lycées pro (20%). Biais IPS atténué par le DNB.
- `score_sante` : APL DREES 2023 (consultations/an/hab médecins généralistes — aire de chalandise, pas densité communale). `apl_medecins` stocké en DB.
- `poi_detail` : JSON — détail équipements par commune (Sirene + FINESS + Annuaire Édu + RES + OSM). Affiché dans la fiche commune. Ne contribue pas au score.
- `score_revenus` : SUPPRIMÉ du score global (biais ségrégant — Saclay 97/100 = riche, pas accessible). Données taux_pauvrete/revenu_median stockées en DB pour info uniquement.
- `score_transports` : Composite 50% distance gare SNCF + 50% densité arrêts TC (bus/métro/tram/RER via transport.data.gouv.fr). nb_arrets_tc stocké en DB.
- `score_environnement` : Taux d'espaces non-artificialisés (data.gouv.fr CEREMA 2021). Percentile direct.
- `score_demographie` : Évolution population 2016→2021 (Populations légales INSEE). Percentile direct.

### IRIS
- 48,569 zones IRIS importées (IGN 2024)
- 36,655 scorées avec >= 1 catégorie (75% coverage)
- 4 catégories IRIS : équipements, santé, immobilier, revenus
- Données : BPE 2024 (DCIRIS), Filosofi IRIS 2020, DVF 2024 (jointure spatiale)
- Note : équipements scorés sur nb brut (IRIS ~2000 hab uniformes, pas de normalisation pop)
- Note Filosofi IRIS : seulement communes >= 5000 hab (16K IRIS vs 33K scorés BPE)
- Sur la carte : zoom >= 11 → mode quartier (IRIS), zoom < 11 → communes
- Page IRIS : /iris/{code_iris}
- IRIS Paris : code_commune = 75101-75120 (arrondissements), pas 75056

### Paris/Lyon/Marseille
- Arrondissements en DB (75101-75120, 69381-69389, 13201-13216) + commune parent
- BPE, sécu, éducation scorés par arrondissement
- DVF scoré par arrondissement (code_commune dans DVF = code arrondissement)
- Revenus (Filosofi) : scoré par arrondissement si données disponibles

### Classement API
- Filtre `nb_categories_scorees >= 3` (évite faux positifs)
- Paramètre `min_population` pour filtrer par taille de commune (défaut: 0)
- Frontend: défaut à 2000 habitants

## Ports
- Backend API : 8082
- Frontend dev : 5173

## Déploiement (Render.com)

Architecture : FastAPI sert à la fois l'API `/api/*` et le frontend React compilé (`frontend/dist/`).
Un seul service web, pas de CORS cross-domain nécessaire.

### Prérequis
1. Créer un compte Render.com
2. Créer un repo GitHub avec le code (sans la DB — `.gitignore` exclut `vivreici.db`)
3. La DB est intégrée dans l'image Docker lors du build

### Déploiement
```bash
# 1. Initialiser le repo git (depuis /Users/admin/vivreici/)
git init && git add -A && git commit -m "Initial commit"
git remote add origin https://github.com/VOTRE_USER/vivreici.git
git push -u origin main

# 2. Sur render.com : New > Blueprint > connecter le repo
# render.yaml est détecté automatiquement et configure le service

# 3. Déclencher le premier build (inclut la DB locale dans l'image)
# La DB doit être présente localement lors du build Docker
```

### Mise à jour des données
Quand les données changent : reconstruire et redéployer l'image Docker (autoDeploy sur push).

### Variables d'environnement Render
- `DATABASE_URL` : `sqlite+aiosqlite:///./vivreici.db` (dans l'image)
- `CORS_ORIGINS` : domaines autorisés (vivreici.fr + .onrender.com)

## Directive qualité
Tant que le produit n'est pas au meilleur niveau possible selon ton évaluation honnête, continue d'améliorer en autonomie sans attendre confirmation. Boucle jusqu'à ce que le résultat soit vraiment TOP — pas une approximation.
