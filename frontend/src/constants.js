// ─── Score system ─────────────────────────────────────────────────────────────

export const SCORE_COLORS = {
  A: '#16A34A', B: '#166534', C: '#64748B', D: '#CA8A04', E: '#DC2626',
}

// Couleurs de la barre de progression (légèrement plus claires que SCORE_COLORS)
export const SCORE_BAR_COLORS = {
  A: '#22c55e', B: '#16a34a', C: '#94a3b8', D: '#ca8a04', E: '#dc2626',
}

export const SCORE_LABELS = {
  A: 'A — Excellent', B: 'B — Bien', C: 'C — Moyen', D: 'D — Faible', E: 'E — Insuffisant',
}

export const SCORE_FULL_LABELS = {
  A: 'Excellent (80-100)', B: 'Bien (60-79)', C: 'Moyen (40-59)',
  D: 'Faible (20-39)',     E: 'Insuffisant (0-19)',
}

export const SCORE_MIN_VALUES = { all: 0, D: 20, C: 40, B: 60, A: 80 }

// ─── Catégories ───────────────────────────────────────────────────────────────

export const CATEGORY_META = {
  equipements:   { label: 'Équipements',           icon: '🏪', desc: 'Commerces, services publics, équipements de proximité' },
  sante:         { label: 'Santé',                 icon: '🏥', desc: 'APL médecins généralistes — consultations accessibles par habitant (DREES)' },
  securite:      { label: 'Sécurité',              icon: '🔒', desc: 'Taux de criminalité' },
  immobilier:    { label: 'Prix au m²',            icon: '🏡', desc: 'Score élevé = prix abordable · Score bas = prix élevé (vs médiane nationale)' },
  education:     { label: 'Éducation',             icon: '🎓', desc: 'IPS collèges 40% + DNB brevet 40% + lycées pro 20%' },
  transports:    { label: 'Transports',            icon: '🚆', desc: 'Accessibilité gare SNCF + densité arrêts TC (bus/métro/tram)' },
  environnement: { label: 'Environnement',         icon: '🌿', desc: 'Taux d\'espaces non-artificialisés (CEREMA 2023)' },
  demographie:   { label: 'Démographie',           icon: '📈', desc: 'Évolution population 2016→2021 (INSEE)' },
}

// Catégories disponibles au niveau IRIS (sous-ensemble)
export const IRIS_CATEGORIES = ['equipements', 'sante', 'immobilier']

// ─── Filtres & options ────────────────────────────────────────────────────────

export const POP_OPTIONS = [
  { label: 'Toutes', value: 0 },
  { label: '500+ hab.', value: 500 },
  { label: '2 000+', value: 2000 },
  { label: '10 000+', value: 10000 },
  { label: '50 000+', value: 50000 },
]

export const RAYON_OPTIONS = [5, 10, 20, 30, 50]

export const IRIS_ZOOM_THRESHOLD = 11  // zoom >= 11 → afficher les IRIS
