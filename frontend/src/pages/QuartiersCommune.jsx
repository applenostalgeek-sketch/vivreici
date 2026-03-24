import { useState, useEffect } from 'react'
import { useParams, Link, useNavigate } from 'react-router-dom'
import Nav from '../components/Nav.jsx'
import { SCORE_COLORS } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'

const SORT_OPTIONS = [
  { key: 'global',      label: 'Score global',   field: 'score_global' },
  { key: 'equipements', label: 'Équipements',    field: ['sous_scores', 'equipements'] },
  { key: 'sante',       label: 'Santé',          field: ['sous_scores', 'sante'] },
  { key: 'immobilier',  label: 'Accessibilité',  field: ['sous_scores', 'immobilier'] },
]

function getScore(iris, field) {
  if (Array.isArray(field)) return iris[field[0]]?.[field[1]] ?? -1
  return iris[field] ?? -1
}

function MiniBar({ value, color }) {
  if (value == null || value < 0) return <span className="text-xs text-ink-light/40">—</span>
  const pct = Math.round(value)
  return (
    <div className="flex items-center gap-1.5 w-16">
      <div className="flex-1 h-1.5 bg-paper rounded-full overflow-hidden">
        <div className="h-full rounded-full" style={{ width: `${pct}%`, backgroundColor: color || '#9CA3AF' }} />
      </div>
      <span className="text-xs font-mono text-ink-light w-6 text-right">{pct}</span>
    </div>
  )
}

export default function QuartiersCommune() {
  const { codeInsee } = useParams()
  const navigate = useNavigate()
  const [commune, setCommune] = useState(null)
  const [iris, setIris] = useState([])
  const [loading, setLoading] = useState(true)
  const [sortKey, setSortKey] = useState('global')
  const [search, setSearch] = useState('')

  usePageMeta({
    title: commune ? `Quartiers de ${commune.nom}` : 'Quartiers',
    description: commune ? `Scores des quartiers IRIS de ${commune.nom}. Comparez équipements, santé, revenus par quartier.` : null,
  })

  useEffect(() => {
    setLoading(true)
    fetch(`/data/communes/${codeInsee}.json`)
      .then(r => r.json())
      .then(com => {
        setCommune(com)
        setIris(com.iris || [])
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [codeInsee])

  const sortOpt = SORT_OPTIONS.find(o => o.key === sortKey)
  const filtered = iris.filter(z => !search || z.nom.toLowerCase().includes(search.toLowerCase()))
  // Zones complètes (>= 2 catégories) triées par score, puis partielles à la fin
  const sorted = [
    ...filtered.filter(z => !z.donnees_partielles).sort((a, b) => getScore(b, sortOpt.field) - getScore(a, sortOpt.field)),
    ...filtered.filter(z => z.donnees_partielles).sort((a, b) => getScore(b, sortOpt.field) - getScore(a, sortOpt.field)),
  ]

  const communeName = commune?.nom || '…'
  const nbScored = iris.filter(z => z.lettre != null).length

  return (
    <div className="min-h-screen">
      <Nav searchPlaceholder="Autre commune…" />

      <main className="max-w-4xl mx-auto px-6 py-10">
        {/* Breadcrumb */}
        <div className="flex items-center gap-2 text-sm text-ink-light mb-6">
          <Link to="/" className="hover:text-ink">Accueil</Link>
          <span>/</span>
          <button onClick={() => navigate(`/commune/${codeInsee}`)} className="hover:text-ink">{communeName}</button>
          <span>/</span>
          <span className="text-ink">Quartiers</span>
        </div>

        {/* Header */}
        <div className="mb-8">
          <h1 className="font-display text-4xl text-ink mb-1">
            Quartiers de <span className="text-score-A">{communeName}</span>
          </h1>
          <p className="text-ink-light">
            {iris.length} quartiers IRIS · {nbScored} scorés
          </p>
        </div>

        {/* Sort + Search controls */}
        <div className="flex flex-wrap items-center gap-3 mb-6">
          <div className="flex items-center gap-1 bg-paper border border-border rounded-xl p-1 flex-wrap">
            {SORT_OPTIONS.map(opt => (
              <button
                key={opt.key}
                onClick={() => setSortKey(opt.key)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                  sortKey === opt.key
                    ? 'bg-white text-ink shadow-sm border border-border'
                    : 'text-ink-light hover:text-ink'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
          <input
            type="text"
            placeholder="Filtrer par nom…"
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="border border-border rounded-xl px-4 py-2 text-sm bg-white text-ink placeholder:text-ink-light focus:outline-none focus:border-ink/40 flex-1 min-w-40"
          />
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-24 text-ink-light gap-3">
            <div className="w-5 h-5 border-2 border-border border-t-ink rounded-full animate-spin" />
            Chargement…
          </div>
        ) : (
          <div className="space-y-1.5">
            {/* Column headers */}
            <div className="hidden md:grid grid-cols-[32px_1fr_90px_64px_64px_64px_64px] gap-3 px-4 py-2 text-xs text-ink-light uppercase tracking-wider">
              <span>#</span>
              <span>Quartier</span>
              <span className="text-center">Score</span>
              <span className="text-center">Equip.</span>
              <span className="text-center">Santé</span>
              <span className="text-center">Access.</span>
              <span className="text-center">Revenus</span>
            </div>

            {sorted.map((z, i) => {
              const barColor = SCORE_COLORS[z.lettre] || '#9CA3AF'
              // Séparateur avant la première zone partielle
              const showSeparator = z.donnees_partielles && (i === 0 || !sorted[i-1].donnees_partielles)
              return (
                <div key={z.code_iris}>
                  {showSeparator && (
                    <div className="flex items-center gap-3 px-2 py-3 mt-2">
                      <div className="flex-1 h-px bg-border" />
                      <span className="text-xs text-ink-light flex-shrink-0">Données partielles (1 catégorie)</span>
                      <div className="flex-1 h-px bg-border" />
                    </div>
                  )}
                  <button
                    onClick={() => navigate(`/iris/${z.code_iris}`)}
                    className="w-full grid grid-cols-[32px_1fr_auto] md:grid-cols-[32px_1fr_90px_64px_64px_64px_64px] gap-3 items-center bg-white border border-border rounded-xl px-4 py-3 hover:border-ink/40 transition-all text-left group"
                  >
                    <span className="font-mono text-sm text-ink-light text-center">
                      {z.rang != null ? z.rang : '—'}
                    </span>

                    <div className="min-w-0">
                      <div className="font-medium text-ink group-hover:underline truncate text-sm">{z.nom}</div>
                      {z.population > 0 && (
                        <div className="text-xs text-ink-light">{z.population.toLocaleString('fr-FR')} hab.</div>
                      )}
                    </div>

                    {/* Score badge */}
                    <div className="flex items-center gap-2 flex-shrink-0">
                      {z.lettre != null ? (
                        <>
                          <span className="font-mono text-sm text-ink-light hidden md:inline">{Math.round(z.score_global)}</span>
                          <div
                            className="w-8 h-8 rounded-lg flex items-center justify-center font-display font-bold text-white text-sm flex-shrink-0"
                            style={{ backgroundColor: barColor }}
                          >
                            {z.lettre}
                          </div>
                        </>
                      ) : z.score_global != null ? (
                        <span className="text-xs text-ink-light hidden md:inline">{Math.round(z.score_global)} pts</span>
                      ) : (
                        <span className="text-xs text-ink-light">—</span>
                      )}
                    </div>

                    {/* Sub-scores — desktop only */}
                    <div className="hidden md:flex justify-center"><MiniBar value={z.sous_scores?.equipements} color="#16A34A" /></div>
                    <div className="hidden md:flex justify-center"><MiniBar value={z.sous_scores?.sante} color="#0EA5E9" /></div>
                    <div className="hidden md:flex justify-center"><MiniBar value={z.sous_scores?.immobilier} color="#8B5CF6" /></div>
                    <div className="hidden md:flex justify-center"><MiniBar value={z.sous_scores?.revenus} color="#F59E0B" /></div>
                  </button>
                </div>
              )
            })}

            {sorted.length === 0 && (
              <div className="text-center py-16 text-ink-light">Aucun quartier trouvé</div>
            )}
          </div>
        )}

        <div className="mt-8 pt-6 border-t border-border text-xs text-ink-light">
          Données IRIS INSEE · BPE 2024, DVF 2024, Filosofi 2021 ·
          <button onClick={() => navigate(`/commune/${codeInsee}`)} className="ml-1 underline hover:text-ink">
            Retour à la fiche commune
          </button>
          {' '}·{' '}
          <Link to="/methode" className="underline hover:text-ink">Méthode</Link>
        </div>
      </main>
    </div>
  )
}
