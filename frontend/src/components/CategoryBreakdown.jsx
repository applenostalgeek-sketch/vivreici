const CATEGORIES = {
  equipements:  { label: 'Équipements & Services', icon: '🏪', description: 'Commerces, services publics, équipements de proximité' },
  securite:     { label: 'Sécurité', icon: '🛡️', description: 'Criminalité pour 1000 hab.' },
  immobilier:   { label: 'Immobilier', icon: '🏠', description: 'Prix au m² médian' },
  education:    { label: 'Éducation', icon: '🎓', description: 'Écoles, lycées, résultats' },
  sante:        { label: 'Santé', icon: '🏥', description: 'Médecins, pharmacies, hôpitaux' },
  environnement:{ label: 'Environnement', icon: '🌿', description: "Qualité de l'air" },
  demographie:  { label: 'Dynamisme', icon: '📈', description: 'Évolution de la population' },
}

function getScoreColor(score) {
  if (score >= 80) return { bar: 'bg-score-A', text: 'text-score-A' }
  if (score >= 60) return { bar: 'bg-score-B', text: 'text-score-B' }
  if (score >= 40) return { bar: 'bg-score-C', text: 'text-score-C' }
  if (score >= 20) return { bar: 'bg-score-D', text: 'text-score-D' }
  return { bar: 'bg-score-E', text: 'text-score-E' }
}

export default function CategoryBreakdown({ sousScores }) {
  if (!sousScores) return null

  const available = Object.entries(sousScores).filter(([, v]) => v !== null && v >= 0)

  return (
    <div className="space-y-3">
      {available.map(([cat, score], i) => {
        const meta = CATEGORIES[cat]
        if (!meta) return null
        const colors = getScoreColor(score)

        return (
          <div
            key={cat}
            className="animate-fade-up"
            style={{ animationDelay: `${i * 60}ms`, opacity: 0, animationFillMode: 'forwards' }}
          >
            <div className="flex items-center justify-between mb-1.5">
              <div className="flex items-center gap-2">
                <span className="text-lg">{meta.icon}</span>
                <span className="font-medium text-ink text-sm">{meta.label}</span>
              </div>
              <span className={`font-mono text-sm font-medium ${colors.text}`}>
                {Math.round(score)}
              </span>
            </div>

            <div className="h-2 bg-border rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-700 ${colors.bar}`}
                style={{ width: `${score}%`, transitionDelay: `${i * 60 + 200}ms` }}
              />
            </div>
          </div>
        )
      })}

      {available.length === 0 && (
        <p className="text-sm text-ink-light text-center py-4">
          Données en cours d'import pour cette commune.
        </p>
      )}
    </div>
  )
}
