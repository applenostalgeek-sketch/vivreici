/**
 * Profils utilisateur — poids par catégorie pour le recalcul du score.
 * Utilisé par ProfileContext (React) et getClassement (useSearch).
 */

export const PROFILES = {
  national: {
    label: 'Score national',
    emoji: null,
    weights: {
      equipements: 0.16, immobilier: 0.20, sante: 0.17,
      transports: 0.15, securite: 0.13, education: 0.08,
      environnement: 0.07, risques: 0.04,
    },
  },
  famille: {
    label: 'Famille',
    emoji: '👪',
    weights: {
      education: 0.22, securite: 0.20, equipements: 0.18,
      immobilier: 0.14, sante: 0.12, transports: 0.10,
      environnement: 0.03, risques: 0.01,
    },
  },
  jeune_actif: {
    label: 'Jeune actif',
    emoji: '⚡',
    weights: {
      transports: 0.30, immobilier: 0.26, equipements: 0.18,
      securite: 0.12, sante: 0.08, education: 0.03,
      environnement: 0.02, risques: 0.01,
    },
  },
  calme: {
    label: 'Calme',
    emoji: '🌿',
    weights: {
      environnement: 0.28, immobilier: 0.20, securite: 0.18,
      sante: 0.15, equipements: 0.10,
      transports: 0.02, education: 0.01, risques: 0.06,
    },
  },
  retraite: {
    label: 'Retraite',
    emoji: '☀️',
    weights: {
      sante: 0.28, securite: 0.18, equipements: 0.14,
      immobilier: 0.12, environnement: 0.12, transports: 0.08,
      risques: 0.04, education: 0.04,
    },
  },
  budget: {
    label: 'Budget',
    emoji: '💰',
    weights: {
      immobilier: 0.40, transports: 0.16, equipements: 0.15,
      sante: 0.12, securite: 0.10, education: 0.04,
      environnement: 0.02, risques: 0.01,
    },
  },
}

export function scoreToLettre(score) {
  if (score >= 80) return 'A'
  if (score >= 60) return 'B'
  if (score >= 40) return 'C'
  if (score >= 20) return 'D'
  return 'E'
}

/**
 * Recalcule le score d'une commune selon les poids d'un profil.
 * sous_scores : { equipements, sante, ... } (valeurs 0-100 ou null)
 * profileKey  : clé dans PROFILES
 * Retourne { score_global, lettre } ou null si profileKey === 'national' ou données insuffisantes.
 */
export function recalcScore(sous_scores, profileKey) {
  if (!sous_scores || profileKey === 'national') return null
  const weights = PROFILES[profileKey]?.weights
  if (!weights) return null

  const available = {}
  for (const [cat, w] of Object.entries(weights)) {
    const val = sous_scores[cat]
    if (val != null && val >= 0) available[cat] = val
  }
  if (Object.keys(available).length === 0) return null

  const totalW = Object.keys(available).reduce((s, cat) => s + weights[cat], 0)
  const score = Object.entries(available).reduce(
    (s, [cat, val]) => s + val * (weights[cat] / totalW), 0
  )
  const rounded = Math.round(score * 10) / 10
  return { score_global: rounded, lettre: scoreToLettre(rounded) }
}
