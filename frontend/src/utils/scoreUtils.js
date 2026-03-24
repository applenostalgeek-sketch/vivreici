import { SCORE_BAR_COLORS } from '../constants.js'

export function scoreToLettre(score) {
  if (score >= 80) return 'A'
  if (score >= 60) return 'B'
  if (score >= 40) return 'C'
  if (score >= 20) return 'D'
  return 'E'
}

export function scoreToColor(score) {
  if (score >= 80) return SCORE_BAR_COLORS.A
  if (score >= 60) return SCORE_BAR_COLORS.B
  if (score >= 40) return SCORE_BAR_COLORS.C
  if (score >= 20) return SCORE_BAR_COLORS.D
  return SCORE_BAR_COLORS.E
}

export function scoreToTextClass(score) {
  if (score >= 80) return 'text-score-A'
  if (score >= 60) return 'text-score-B'
  if (score >= 40) return 'text-score-C'
  if (score >= 20) return 'text-score-D'
  return 'text-score-E'
}

export function scoreToPercentileLabel(score) {
  if (score >= 80) return 'Top 20%'
  if (score >= 50) return 'Au-dessus médiane'
  if (score >= 20) return 'En dessous médiane'
  return 'Bottom 20%'
}
