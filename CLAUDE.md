# VivreIci — Instructions de développement

## Contexte
Plateforme de scoring de qualité de vie des communes françaises basée sur l'open data.
Score A-E par commune (style Nutri-Score) — 8 catégories.

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
5. `python -m backend.data_import.import_securite`    → criminalité (SSMSI 2025)
6. `python -m backend.data_import.import_education`   → IPS collèges + DNB + lycées pro
7. `python -m backend.data_import.import_dvf`         → immobilier (DVF 2024, ~3min)
7b. `python3 -m backend.data_import.import_notaires_alsace` → prix notaires Alsace-Moselle 67/68/57 (après DVF)
8. `python -m backend.data_import.import_filosofi`    → revenus/pauvreté (Filosofi 2021)
9. `python3 -m backend.data_import.import_transports`  → score transports (gares SNCF, ~1min)
10. `python3 -m backend.data_import.import_demographie` → évolution population 2016→2021
11. `python3 -m backend.data_import.import_environnement` → artificialisation sols (CEREMA/data.gouv.fr)
12. `python3 -m backend.data_import.import_qualite_air`   → qualité air ATMO France (~15min, WFS public, 12 mois glissants)
13. `python3 -m backend.data_import.import_apl`         → santé APL DREES 2023 (remplace BPE medecins)
14. `python3 -m backend.data_import.import_risques`     → risques naturels PPR (GASPAR Géorisques, ~5min)

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
5. `python3 -m backend.data_import.import_commune_to_iris`  → transfère sécu/transports/édu commune→IRIS + fallback santé (~30s) **OBLIGATOIRE**
6. `python3 -m backend.data_import.import_iris_geometry`    → contours polygons IRIS pour la carte (~5min, optionnel)

## Architecture

### Backend DB (SQLite vivreici.db)
- `communes` : code_insee, nom, departement, region, population, codes_postaux, latitude, longitude
- `scores` : 8 sous-scores (-1 si absent), données brutes, score_global, lettre, nb_categories_scorees
- `iris_zones` : code_iris (9 chars = commune 5 + iris 4), nom, code_commune, typ_iris, population, latitude, longitude
- `iris_scores` : 6 sous-scores IRIS (equipements, sante, immobilier — locaux + securite, transports, education — injectés depuis commune), score_global, lettre

### Scores (0-100, percentile national)
- `score_equipements` : BPE 2024 — **score hybride présence + densité**, pondéré par population.
  - **Présence** : `min(count, 1) × poids` → somme = `presence_score`. Mesure la variété des services (avoir 50 pharmacies = avoir 1).
  - **Densité** : `sum(count × poids)` = `weighted_count` → densité pour 10 000 hab. Mesure la capacité réelle.
  - **Score hybride** : `alpha × percentile_présence + (1-alpha) × percentile_densité` où `alpha = 1/(1+(pop/K)^P)`, K=20000, P=1.5.
    - Village 500 hab : α≈1.0 → quasi 100% présence (variété des services).
    - Ville 20k hab : α=0.5 → 50/50.
    - Grande ville 100k+ : α≈0.04 → quasi 100% densité (services par habitant).
  - **Pourquoi** : la présence seule saturait à 26/26 pour toutes les communes >10k hab → Paris scorait 95+. Le score hybride différencie les grandes villes entre elles par la densité, tout en protégeant les petites communes du biais densité.
  - Poids : pharmacie(4), supermarché(4), hypermarché(4), boulangerie(2), poste(2), hôpital(2), urgences(2), boucherie(1), gymnase(1), piscine(1), cinéma(1), bibliothèque(1), théâtre(1). Score max théorique : 26 pts.
  - Exclu du score : médecins (→ score_sante), mairie (partout), écoles (→ score_education), terrain_football (trop répandu), gare (→ score_transports).
  - Script de recalcul sans re-télécharger BPE : `python3 -m scripts.recalc_equipements_hybride`
- `score_sante` : BPE 2024 (médecins pour 10000 hab)
- `score_securite` : SSMSI 2025 (taux criminalité pour mille, sens inverse — moins de crimes = mieux).
  - **6 catégories** : cambriolages, violences physiques (intra+hors famille), vols sans violence, vols violents sans arme, vols avec armes.
  - **Secret statistique** : communes avec ≤5 faits sur 3 ans consécutifs → `ndiff` (non diffusé). Le fichier SSMSI fournit `complement_info_taux` = moyenne départementale des communes ndiff. Utilisé comme estimation.
  - **Avant fix (avr. 2026)** : ndiff traité comme 0 → 82% des communes scoraient 100 (distribution binaire). Corrigé : 4.2% à 99.5+, distribution uniforme.
  - Percentile national inversé (somme des 6 taux).
- `score_immobilier` : DVF 2024 (prix m² médian, sens inverse — moins cher = score plus élevé).
  - **Source primaire** : DVF (Demandes de Valeurs Foncières) — transactions réelles, 24 298 communes (~69%).
  - **Alsace-Moselle (67/68/57)** : pas de DVF (livre foncier local). Source alternative = **API notaires** (immobilier.notaires.fr).
    - Endpoint `/pub-services/immodecret-stat1/v1/prix` — prix médians par commune, données notariales réelles.
    - ~94 communes avec données directes (appart + maison, moyenne pondérée par nb transactions).
    - Communes rurales sans données notaires : KNN local basé sur les communes notaires du même département (max 50 km, 8 voisins, 1/d²).
    - Script : `python3 -m backend.data_import.import_notaires_alsace` (à lancer APRÈS import_dvf).
    - **Avant fix (avr. 2026)** : KNN depuis départements voisins → résultats aberrants (Mulhouse estimé 3 057 €/m² vs 1 317 € réel).
  - **Fallback KNN spatial** : pour les autres communes sans DVF (hors Alsace-Moselle).
    - Méthode : KDTree par tranche de population (<500, 500-2k, 2k-10k, 10k-50k, 50k+). Pour chaque commune manquante, moyenne pondérée (1/d²) des 10 communes DVF les plus proches dans la même tranche.
    - Pourquoi stratifié : évite qu'un village rural copie le prix d'une ville voisine (et inversement).
  - `prix_m2_estime = 1` en DB pour identifier les estimations (vs données DVF/notaires réelles).
  - Couverture finale : 99.9% (35 377 communes).
  - Percentile calculé sur l'ensemble (DVF + notaires + estimés).
  - `MIN_TRANSACTIONS = 5` : communes DVF avec < 5 transactions exclues (médiane non fiable).
- `score_education` : Score APL-style sur rayon 30km. Pour chaque commune, agrège tous les établissements dans un rayon de 30km pondérés par 1/distance (distance min clampée à 1km pour éviter div/0).
  - **Qualité (90%)** : composite IPS collèges 2024-2025 (40%) + DNB brevet 2021 (40%) + lycées pro (20%). Percentile national.
  - **Proximité (10%)** : percentile inverse de la distance au collège le plus proche. Percentile national.
  - `score_education` = 0.9 × score_qualité + 0.1 × score_proximité
  - `min_dist_college_km` : distance au collège le plus proche (stocké en DB, exporté en donnees_brutes).
  - Couvre ~100% des communes (vs ~11% avant — avant : uniquement communes avec leur propre collège).
- `score_sante` : APL DREES 2023 (consultations/an/hab médecins généralistes — aire de chalandise, pas densité communale). `apl_medecins` stocké en DB.
- `poi_detail` : JSON — détail équipements par commune (Sirene + FINESS + Annuaire Édu + RES + OSM). Affiché dans la fiche commune. Ne contribue pas au score.
- `score_revenus` : SUPPRIMÉ du score global (biais ségrégant — Saclay 97/100 = riche, pas accessible). Données taux_pauvrete/revenu_median stockées en DB pour info uniquement.
- `score_transports` : Composite 50% distance gare SNCF + 50% densité arrêts TC (bus/métro/tram/RER via transport.data.gouv.fr). nb_arrets_tc stocké en DB.
- `score_environnement` : Composite 50% artificialisation + 50% qualité air.
  - Artificialisation : taux d'espaces non-artificialisés CEREMA (data.gouv.fr). Stocké aussi en `taux_espaces_nat`.
  - Qualité air : indice ATMO moyen annuel (Atmo France, WFS public). `score_qualite_air`, `qualite_air_moy`, `qualite_air_nb_jours` stockés en DB.
  - Si commune sans données qualité air (37% des communes, surtout rurales) : score environnement = artificialisation seule.
  - Mise à jour : relancer `import_environnement` puis `import_qualite_air` (ordre important — qualite_air recalcule le composite).
- `score_demographie` : RETIRÉ du score global (autoréférentiel — la croissance est une conséquence, pas un critère). Données conservées en DB pour affichage uniquement.
- `score_risques` : PPR naturels approuvés GASPAR — composite inondation(35%)+séisme(30%)+MVT(20%)+forêt(10%)+avalanche(5%). Percentile inversé.

### IRIS
- 48,569 zones IRIS importées (IGN 2024)
- 40,860 scorées avec >= 2 catégories (99% coverage des IRIS avec données locales)
- **8 catégories IRIS** : équipements, santé, immobilier (locaux) + sécurité, transports, éducation, environnement, risques (injectés depuis commune)
- **Même méthodologie et poids que les communes** (CATEGORIES dict) → scores comparables
- `import_commune_to_iris.py` : transfère les 3 sous-scores commune + fallback santé APL
- `score_revenus` : stocké en DB mais EXCLU du score (cohérence avec communes)
- Données locales : BPE 2024 (DCIRIS), DVF 2024 (jointure spatiale)
- Note : équipements scorés sur nb brut (IRIS ~2000 hab uniformes, pas de normalisation pop)
- Note Filosofi IRIS : taux_pauvrete disponible communes >= 5000 hab — stocké en données brutes, non scoré
- Sur la carte : zoom >= 11 → mode quartier (IRIS), zoom < 11 → communes
- Page IRIS : /iris/{code_iris} — affiche contexte commune (lettre + score global)
- IRIS Paris : code_commune = 75101-75120 (arrondissements), pas 75056
- **Bug connu** : Limoges (87085) population=0 en DB → score_equipements=0 pour la commune (score C anormalement bas). IRIS de Limoges scorent correctement (6 cats, B-C). Bug de données, non lié au scoring IRIS.

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
