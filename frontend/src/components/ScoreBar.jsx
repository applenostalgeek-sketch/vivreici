import { scoreToColor, scoreToTextClass, scoreToPercentileLabel } from '../utils/scoreUtils.js'

/**
 * Barre de score horizontal pour une catégorie.
 * Utilisé dans Commune.jsx et Iris.jsx.
 */
export default function ScoreBar({ value, label, icon, desc }) {
  if (value == null) return null
  const pct = Math.round(value)
  const color = scoreToColor(pct)
  const textClass = scoreToTextClass(pct)

  return (
    <div className="flex items-center gap-3">
      <span className="text-lg w-6 flex-shrink-0">{icon}</span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-1">
          <span className="text-sm font-medium text-ink">{label}</span>
          <span className={`text-sm font-mono font-semibold ${textClass}`}>
            {pct}<span className="text-xs text-ink-light font-normal">/100</span>
          </span>
        </div>
        <div className="w-full h-2 bg-paper rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700"
            style={{ width: `${pct}%`, backgroundColor: color }}
          />
        </div>
        <div className="flex justify-between text-xs text-ink-light mt-0.5">
          <span>{desc}</span>
          <span className="font-medium">{scoreToPercentileLabel(pct)}</span>
        </div>
      </div>
    </div>
  )
}
