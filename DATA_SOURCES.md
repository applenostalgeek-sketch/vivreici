# VivreIci — Sources de données & mise à jour

## Vue d'ensemble

Toutes les données sont des **open data françaises**, sans clé API payante.
La base de données est un fichier SQLite local (`vivreici.db`) mis à jour manuellement.
Toutes les commandes se lancent depuis `/Users/admin/vivreici/`.

---

## 1. Géographie des communes

### Ce que ça couvre
Table `communes` : code INSEE, nom, département, région, population, codes postaux, coordonnées GPS.

### Sources
| Script | Source | URL |
|--------|--------|-----|
| `import_geo.py` | API Géo (Etalab) | https://geo.api.gouv.fr/communes |
| `import_geo_fallback.py` | GeoJSON communes (data.gouv.fr) | Téléchargé automatiquement |
| `import_coords.py` | Coordonnées GPS (data.gouv.fr) | Téléchargé automatiquement |

### Fréquence de mise à jour
Annuelle — après le recensement INSEE (novembre). Les communes fusionnent ou changent de code rarement.

### Comment mettre à jour
```bash
python -m backend.data_import.import_geo
python -m backend.data_import.import_geo_fallback
python -m backend.data_import.import_coords
```

---

## 2. Équipements (score_equipements)

### Ce que ça couvre
Nombre d'équipements pour 1 000 habitants (commerces, services, santé BPE).
Contribue au **score_equipements** (percentile national).

### Source
**BPE 2024** — Base Permanente des Équipements (INSEE)
- URL : https://www.insee.fr/fr/statistiques/3568638
- Fichier : `bpe24_ensemble_xy_csv.zip` (~200 Mo)
- Mise à jour : **annuelle** (publication automne)

### Comment mettre à jour
```bash
python -m backend.data_import.import_bpe
```
> Remplacer l'année dans le script si la version change (ex: `bpe25_...`).

---

## 3. Santé — Médecins généralistes (score_sante)

### Ce que ça couvre
APL (Accessibilité Potentielle Localisée) : consultations/an/habitant pour les médecins généralistes.
Mesure l'accès réel, pas la densité communale brute.
Contribue au **score_sante**.

### Source
**APL DREES 2023**
- URL : https://data.drees.solidarites-sante.gouv.fr/ (rechercher "APL")
- Fichier : CSV communes avec colonne `apl_genmvt_covid` ou équivalent
- Mise à jour : **tous les 2 ans** environ

### Comment mettre à jour
```bash
python -m backend.data_import.import_apl
```

---

## 4. Sécurité (score_securite)

### Ce que ça couvre
Taux de criminalité (délits pour 1 000 habitants).
Contribue au **score_securite** (score inversé : moins de crime = meilleur score).

### Source
**SSMSI 2024** — Service Statistique Ministériel de la Sécurité Intérieure
- URL : https://www.interieur.gouv.fr/Interstats/Actualites/
- Fichier : CSV délinquance par commune
- Mise à jour : **annuelle** (publication printemps)

### Comment mettre à jour
```bash
python -m backend.data_import.import_securite
```

---

## 5. Éducation (score_education)

### Ce que ça couvre
Score composite : IPS collèges 2024-2025 (40%) + DNB brevet 2021 (40%) + lycées pro (20%).
Contribue au **score_education**.

### Sources
| Composante | Source | URL |
|------------|--------|-----|
| IPS collèges | DEPP / data.education.gouv.fr | Rechercher "IPS" |
| DNB résultats | DEPP / data.education.gouv.fr | Rechercher "DNB" |
| Lycées pro | data.education.gouv.fr | — |

### Comment mettre à jour
```bash
python -m backend.data_import.import_education
```

---

## 6. Immobilier (score_immobilier)

### Ce que ça couvre
Prix médian au m² (appartements + maisons), avec tendance 2022→2024.
Contribue au **score_immobilier** (score inversé : prix bas = plus accessible).

### Source
**DVF 2024** — Demandes de Valeurs Foncières (DGFiP)
- URL : https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
- Fichier : `full.csv.gz` (~2 Go) — téléchargé automatiquement
- Mise à jour : **annuelle** (publication avril)

### Comment mettre à jour
```bash
python -m backend.data_import.import_dvf          # Prix 2024
python -m backend.data_import.import_dvf_historique  # Prix 2022 (tendance)
```

---

## 7. Revenus & pauvreté (info seulement, hors score)

### Ce que ça couvre
Revenu médian et taux de pauvreté par commune. Affiché en "données brutes" mais **ne contribue pas au score** (biais ségrégant écarté volontairement).

### Source
**Filosofi 2021** — Fichier localisé social et fiscal (INSEE)
- URL : https://www.insee.fr/fr/statistiques/6049648
- Mise à jour : **tous les 2 ans**

### Comment mettre à jour
```bash
python -m backend.data_import.import_filosofi
```

---

## 8. Transports (score_transports)

### Ce que ça couvre
Score composite : distance à la gare SNCF (50%) + densité arrêts TC bus/métro/tram (50%).
Contribue au **score_transports**.

### Sources
| Composante | Source |
|------------|--------|
| Gares SNCF | data.sncf.com (liste gares voyageurs) |
| Arrêts TC | transport.data.gouv.fr (GTFS agrégé) |

### Comment mettre à jour
```bash
python -m backend.data_import.import_transports
```

---

## 9. Démographie (score_demographie)

### Ce que ça couvre
Évolution de la population 2016→2021. Percentile direct.
Contribue au **score_demographie**.

### Source
**Populations légales INSEE** (2016 + 2021)
- URL : https://www.insee.fr/fr/statistiques/

### Comment mettre à jour
```bash
python -m backend.data_import.import_demographie
```

---

## 10. Environnement (score_environnement)

### Ce que ça couvre
Taux d'espaces non-artificialisés. Percentile direct.
Contribue au **score_environnement**.

### Source
**CEREMA 2021** — Artificialisation des sols (data.gouv.fr)
- URL : https://www.data.gouv.fr/

### Comment mettre à jour
```bash
python -m backend.data_import.import_environnement
```

---

## 11. Présence d'équipements par commune (poi_detail)

> Affiché en tags sur la fiche commune. **Ne contribue pas au score.**

### Ce que ça couvre
21 catégories : santé, éducation, sports, culture, commerce.
Stocké en JSON dans `scores.poi_detail`.

### Sources et catégories couvertes

| Script | Source | Catégories |
|--------|--------|------------|
| `import_finess.py` | FINESS data.gouv.fr | pharmacie, hôpital, clinique, cabinet_médical, labo_analyse |
| `import_education_poi.py` | Annuaire éducation nationale | école_maternelle, école_primaire, collège, lycée, lycée_professionnel |
| `import_res.py` | RES equipements.sports.gouv.fr | piscine, gymnase, stade |
| `import_culture_osm.py` | OpenStreetMap (Overpass API) | cinéma, bibliothèque, théâtre, boulangerie, supermarché, boucherie |
| `import_musees_osm.py` | OpenStreetMap (tourism=museum) | musée |

### Fréquence de mise à jour
- FINESS : annuelle (mars)
- Éducation : annuelle (rentrée)
- RES : tous les 2-3 ans
- OSM : à la demande (données contributives, évoluent en continu)

### Comment mettre à jour (ordre recommandé)
```bash
python3 -m backend.data_import.import_finess
python3 -m backend.data_import.import_education_poi
python3 -m backend.data_import.import_res
python3 -m backend.data_import.import_culture_osm    # ~5-10 min, Overpass API
python3 -m backend.data_import.import_musees_osm     # ~5 min
python3 -m backend.data_import.import_osm_retry      # si des départements ont échoué
```

> **Note OSM** : l'Overpass API publique est parfois surchargée (erreurs 504/429).
> `import_osm_retry.py` détecte et relance automatiquement les départements manqués.
> Le script utilise 3 endpoints en rotation : overpass-api.de, overpass.karte.io, overpass.openstreetmap.ru.

---

## 12. Zones IRIS & scores par quartier

### Ce que ça couvre
48 569 zones IRIS (quartiers ~2 000 hab.), avec 4 scores : équipements, santé, immobilier, revenus.

### Sources

| Script | Source | Contenu |
|--------|--------|---------|
| `import_iris_zones.py` | IGN 2024 (data.gouv.fr) | Contours + centroides IRIS |
| `import_bpe_iris.py` | BPE 2024 INSEE (DCIRIS) | Équipements par IRIS |
| `import_filosofi_iris.py` | Filosofi 2020 INSEE | Revenus par IRIS |
| `import_dvf_iris.py` | DVF 2024 DGFiP | Immobilier par IRIS (~15 min) |
| `import_iris_geometry.py` | IGN 2024 | Polygones pour la carte (optionnel) |

### Comment mettre à jour
```bash
python3 -m backend.data_import.import_iris_zones
python3 -m backend.data_import.import_bpe_iris
python3 -m backend.data_import.import_filosofi_iris
python3 -m backend.data_import.import_dvf_iris
python3 -m backend.data_import.import_iris_geometry   # optionnel
```

---

## 13. Présence d'équipements par quartier IRIS (poi_detail IRIS)

> Affiché en tags sur la fiche quartier. **Ne contribue pas au score.**

### Ce que ça couvre
16 catégories : éducation, sports, culture, commerce.
Stocké en JSON dans `iris_scores.poi_detail`.
Attribution par GPS : chaque POI est assigné à l'IRIS le plus proche (< 2 km).

### Sources

| Script | Source | Catégories |
|--------|--------|------------|
| `import_poi_iris.py` | Annuaire éducation + RES | école_maternelle, école_primaire, collège, lycée, lycée_professionnel, piscine, gymnase, stade |
| `import_culture_osm.py` | OSM Overpass | cinéma, bibliothèque, théâtre, boulangerie, supermarché, boucherie |
| `import_musees_osm.py` | OSM tourism=museum | musée |

### Comment mettre à jour
```bash
python3 -m backend.data_import.import_poi_iris        # ~10 min (télécharge 350 Mo RES)
python3 -m backend.data_import.import_culture_osm     # met à jour communes + IRIS simultanément
python3 -m backend.data_import.import_musees_osm      # idem
```

---

## Ordre de mise à jour complète (tout refaire)

```bash
# 1. Géographie (si nouvelles communes)
python -m backend.data_import.import_geo
python -m backend.data_import.import_geo_fallback
python -m backend.data_import.import_coords

# 2. Scores communes
python -m backend.data_import.import_bpe
python -m backend.data_import.import_apl
python -m backend.data_import.import_securite
python -m backend.data_import.import_education
python -m backend.data_import.import_dvf
python -m backend.data_import.import_dvf_historique
python -m backend.data_import.import_filosofi
python3 -m backend.data_import.import_transports
python3 -m backend.data_import.import_demographie
python3 -m backend.data_import.import_environnement

# 3. POI communes
python3 -m backend.data_import.import_finess
python3 -m backend.data_import.import_education_poi
python3 -m backend.data_import.import_res
python3 -m backend.data_import.import_culture_osm
python3 -m backend.data_import.import_musees_osm
python3 -m backend.data_import.import_osm_retry       # si manques OSM

# 4. IRIS (après communes)
python3 -m backend.data_import.import_iris_zones
python3 -m backend.data_import.import_bpe_iris
python3 -m backend.data_import.import_filosofi_iris
python3 -m backend.data_import.import_dvf_iris
python3 -m backend.data_import.import_iris_geometry   # optionnel
python3 -m backend.data_import.import_poi_iris
```

---

## Points d'attention

### Encodage FINESS
Le fichier FINESS est en **UTF-8** (pas latin-1). Le script gère ça automatiquement avec fallback.

### OSM Overpass
L'API publique est parfois surchargée en journée. Préférer **le soir ou le week-end** pour les imports OSM.
En cas d'échec, `import_osm_retry.py` identifie automatiquement les départements manqués.

### Musées OSM
Les musées utilisent le tag `tourism=museum` (pas `amenity=museum`). C'est pour ça qu'ils ont un script dédié (`import_musees_osm.py`).

### Paris / Lyon / Marseille
Les arrondissements ont leur propre code INSEE (75101-75120, 69381-69389, 13201-13216).
Les scores BPE, DVF, sécurité, éducation sont calculés par arrondissement.

### Rollback POI
Pour supprimer l'affichage des équipements sans toucher à la DB :
- Commune : retirer le bloc `poi_detail` dans `Commune.jsx` (~ligne 254)
- Quartier : retirer le bloc `poi_detail` dans `Iris.jsx` (~ligne 274)
- API : retirer `"poi_detail"` dans `main.py` (2 occurrences)
