const SCORE_CONFIG = {
  A: { label: 'Excellent', bg: 'bg-score-A', text: 'text-score-A', light: 'bg-green-50', border: 'border-green-200' },
  B: { label: 'Bien', bg: 'bg-score-B', text: 'text-score-B', light: 'bg-lime-50', border: 'border-lime-200' },
  C: { label: 'Moyen', bg: 'bg-score-C', text: 'text-score-C', light: 'bg-amber-50', border: 'border-amber-200' },
  D: { label: 'Faible', bg: 'bg-score-D', text: 'text-score-D', light: 'bg-orange-50', border: 'border-orange-300' },
  E: { label: 'Insuffisant', bg: 'bg-score-E', text: 'text-score-E', light: 'bg-red-50', border: 'border-red-200' },
}

const TOUTES_LETTRES = ['A', 'B', 'C', 'D', 'E']

export default function ScoreCard({ lettre, score, size = 'lg' }) {
  const config = SCORE_CONFIG[lettre] || SCORE_CONFIG.C
  const isLg = size === 'lg'

  return (
    <div className="flex flex-col items-center gap-6">
      {/* Badge principal */}
      <div className="relative animate-scale-in">
        <div className={`
          ${config.bg} text-white font-display font-bold rounded-[28px] flex items-center justify-center
          shadow-lg
          ${isLg ? 'w-32 h-32 text-7xl' : 'w-20 h-20 text-5xl'}
        `}>
          {lettre}
        </div>
        {/* Score numérique */}
        {score !== undefined && (
          <div className="absolute -bottom-3 left-1/2 -translate-x-1/2 bg-white border border-border rounded-full px-3 py-0.5 text-xs font-mono font-medium text-ink-light whitespace-nowrap shadow-sm">
            {Math.round(score)}/100
          </div>
        )}
      </div>

      {/* Label */}
      <p className={`font-semibold ${config.text} ${isLg ? 'text-lg' : 'text-base'}`}>
        {config.label}
      </p>

      {/* Barre style Nutri-Score */}
      <div className="flex gap-1.5 items-end">
        {TOUTES_LETTRES.map((l) => {
          const c = SCORE_CONFIG[l]
          const isActive = l === lettre
          return (
            <div
              key={l}
              className={`
                flex items-center justify-center rounded-lg font-display font-bold text-white transition-all
                ${c.bg}
                ${isActive
                  ? 'w-10 h-10 text-base ring-2 ring-offset-2 ring-offset-paper ring-current opacity-100'
                  : 'w-8 h-8 text-sm opacity-30'
                }
              `}
              style={isActive ? { ringColor: c.bg.replace('bg-', '') } : {}}
            >
              {l}
            </div>
          )
        })}
      </div>
    </div>
  )
}
