import { useState, useEffect, useCallback } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { getClassement } from '../hooks/useSearch.js'
import Nav from '../components/Nav.jsx'
import { POP_OPTIONS, CATEGORY_META } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'

const SCORE_BAR = { A: 'bg-score-A', B: 'bg-score-B', C: 'bg-score-C', D: 'bg-score-D', E: 'bg-score-E' }
const SCORE_TEXT = { A: 'text-score-A', B: 'text-score-B', C: 'text-score-C', D: 'text-score-D', E: 'text-score-E' }

// Catégories affichées en pills dans le classement
const CAT_LABELS = { equipements: 'Équipements', securite: 'Sécurité', immobilier: 'Prix m²', education: 'Éducation', sante: 'Santé', transports: 'Transports' }

const CAT_FILTERS = [
  { key: 'securite',    label: 'Sécurité',     icon: '🔒' },
  { key: 'transports',  label: 'Transports',    icon: '🚆' },
  { key: 'sante',       label: 'Santé',         icon: '🏥' },
  { key: 'education',   label: 'Éducation',     icon: '🎓' },
  { key: 'equipements', label: 'Équipements',   icon: '🏪' },
  { key: 'immobilier',  label: 'Prix abordable',icon: '🏡' },
]

export default function Classement() {
  usePageMeta({
    title: 'Classement national',
    description: 'Classement des communes françaises par score de qualité de vie. Filtrez par département, taille et critères.',
  })

  const [searchParams] = useSearchParams()
  const [communes, setCommunes] = useState([])
  const [loading, setLoading] = useState(true)
  const [minPop, setMinPop] = useState(2000)
  const [dept, setDept] = useState(() => searchParams.get('departement') || '')
  const [deptInput, setDeptInput] = useState(() => searchParams.get('departement') || '')
  const [catFilters, setCatFilters] = useState({})
  const [hasMore, setHasMore] = useState(false)
  const [offset, setOffset] = useState(0)
  const navigate = useNavigate()
  const LIMIT = 50

  const toggleCat = (key) => {
    setCatFilters(prev => ({ ...prev, [key]: prev[key] ? undefined : 60 }))
  }

  const fetchData = useCallback(async (newOffset = 0, append = false) => {
    setLoading(true)
    try {
      const params = { limit: LIMIT, sort: 'score', ordre: 'desc', min_population: minPop, offset: newOffset }
      if (dept) params.departement = dept
      Object.entries(catFilters).forEach(([key, val]) => { if (val) params[`${key}_min`] = val })
      const data = await getClassement(params)
      if (append) {
        setCommunes(prev => [...prev, ...data])
      } else {
        setCommunes(data)
      }
      setHasMore(data.length === LIMIT)
      setOffset(newOffset + data.length)
    } catch {
      if (!append) setCommunes([])
    } finally {
      setLoading(false)
    }
  }, [minPop, dept, catFilters])

  useEffect(() => {
    setOffset(0)
    fetchData(0, false)
  }, [fetchData])

  const handleDeptSubmit = (e) => {
    e.preventDefault()
    setDept(deptInput.trim().toUpperCase())
  }

  const clearDept = () => {
    setDept('')
    setDeptInput('')
  }

  return (
    <div className="min-h-screen bg-paper">
      <Nav searchPlaceholder="Commune…" />

      <main className="max-w-4xl mx-auto px-6 py-10">
        <div className="mb-8">
          <h1 className="font-display text-4xl text-ink mb-1">Classement national</h1>
          <p className="text-ink-light">Communes classées par score de qualité de vie</p>
        </div>

        {/* Filters */}
        <div className="bg-white border border-border rounded-2xl p-5 mb-8 space-y-4">
          {/* Population filter */}
          <div>
            <p className="text-xs font-medium text-ink-light uppercase tracking-wider mb-2">Population minimale</p>
            <div className="flex flex-wrap gap-2">
              {POP_OPTIONS.map(opt => (
                <button
                  key={opt.value}
                  onClick={() => setMinPop(opt.value)}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium border transition-all ${
                    minPop === opt.value
                      ? 'bg-ink text-white border-ink'
                      : 'border-border text-ink-light hover:border-ink hover:text-ink'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Category filters */}
          <div>
            <p className="text-xs font-medium text-ink-light uppercase tracking-wider mb-2">
              Critères minimum B <span className="normal-case font-normal">(≥ 60/100)</span>
            </p>
            <div className="flex flex-wrap gap-2">
              {CAT_FILTERS.map(({ key, label, icon }) => {
                const active = !!catFilters[key]
                return (
                  <button
                    key={key}
                    onClick={() => toggleCat(key)}
                    className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium border transition-all ${
                      active
                        ? 'bg-ink text-white border-ink'
                        : 'border-border text-ink-light hover:border-ink hover:text-ink'
                    }`}
                  >
                    <span>{icon}</span>
                    <span>{label}</span>
                    {active && <span className="font-mono text-xs opacity-70">B+</span>}
                  </button>
                )
              })}
            </div>
          </div>

          {/* Dept filter */}
          <div className="flex items-center gap-3">
            <p className="text-xs font-medium text-ink-light uppercase tracking-wider whitespace-nowrap">Département</p>
            <form onSubmit={handleDeptSubmit} className="flex items-center gap-2">
              <input
                type="text"
                value={deptInput}
                onChange={e => setDeptInput(e.target.value)}
                placeholder="ex. 78, 75, 2A…"
                maxLength={3}
                className="w-28 border border-border rounded-lg px-3 py-1.5 text-sm font-mono focus:outline-none focus:border-ink"
              />
              <button type="submit" className="px-3 py-1.5 text-sm bg-ink text-white rounded-lg hover:bg-ink/80 transition-colors">
                Filtrer
              </button>
              {dept && (
                <button type="button" onClick={clearDept} className="text-sm text-ink-light hover:text-ink underline">
                  Effacer
                </button>
              )}
            </form>
            {dept && (
              <span className="text-sm text-ink font-mono bg-paper border border-border rounded px-2 py-0.5">
                Dept {dept}
              </span>
            )}
          </div>
        </div>

        {loading && communes.length === 0 ? (
          <div className="flex items-center justify-center py-20 text-ink-light gap-3">
            <div className="w-5 h-5 border-2 border-border border-t-ink rounded-full animate-spin" />
            Chargement…
          </div>
        ) : communes.length === 0 ? (
          <div className="text-center py-20 text-ink-light">
            <p className="text-lg font-display text-ink mb-2">Aucun résultat</p>
            <p className="text-sm">Essayez un autre filtre ou département.</p>
          </div>
        ) : (
          <>
            <div className="text-xs text-ink-light mb-4 font-mono">
              {communes.length} commune{communes.length > 1 ? 's' : ''} affichée{communes.length > 1 ? 's' : ''}
              {minPop > 0 && ` · ${POP_OPTIONS.find(o => o.value === minPop)?.label}`}
              {dept && ` · Dept ${dept}`}
            </div>

            <div className="space-y-2">
              {communes.map((commune, i) => {
                const s = commune.score
                const lettre = s?.lettre || 'C'
                const score = s?.score_global || 0
                const cats = s?.sous_scores || {}

                return (
                  <button
                    key={commune.code_insee}
                    onClick={() => navigate(`/commune/${commune.code_insee}`)}
                    className="w-full bg-white border border-border rounded-xl px-5 py-4 hover:border-ink/40 hover:shadow-sm transition-all duration-150 text-left group"
                  >
                    <div className="flex items-center gap-4">
                      {/* Rank */}
                      <span className="font-mono text-sm text-border w-8 text-right flex-shrink-0">{i + 1}</span>

                      {/* Name + info */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-baseline gap-2 flex-wrap">
                          <span className="font-semibold text-ink group-hover:underline truncate">{commune.nom}</span>
                          <span className="text-xs font-mono text-ink-light flex-shrink-0">{commune.departement}</span>
                          {commune.population > 0 && (
                            <span className="text-xs text-ink-light hidden sm:block flex-shrink-0">
                              {commune.population.toLocaleString('fr-FR')} hab.
                            </span>
                          )}
                        </div>

                        {/* Sub-score pills */}
                        <div className="flex gap-1 mt-1.5 flex-wrap">
                          {Object.entries(CAT_LABELS).map(([key, label]) => {
                            const val = cats[key]
                            if (val == null) return null
                            const pct = Math.round(val)
                            return (
                              <span key={key} className="inline-flex items-center gap-1 text-xs text-ink-light bg-paper border border-border/60 rounded px-1.5 py-0.5">
                                <span className="hidden lg:inline">{label}</span>
                                <span className="font-mono">{pct}</span>
                              </span>
                            )
                          })}
                        </div>
                      </div>

                      {/* Score */}
                      <div className="flex items-center gap-3 flex-shrink-0">
                        {/* Score bar */}
                        <div className="hidden sm:flex flex-col gap-1 items-end">
                          <div className="w-24 h-1.5 bg-paper rounded-full overflow-hidden">
                            <div
                              className={`h-full rounded-full ${SCORE_BAR[lettre]}`}
                              style={{ width: `${score}%` }}
                            />
                          </div>
                        </div>
                        <span className={`font-mono text-sm font-semibold ${SCORE_TEXT[lettre]}`}>{Math.round(score)}</span>
                        <div className={`score-badge w-8 h-8 text-sm flex-shrink-0 score-badge-${lettre}`}>
                          {lettre}
                        </div>
                      </div>
                    </div>
                  </button>
                )
              })}
            </div>

            {hasMore && (
              <div className="mt-6 text-center">
                <button
                  onClick={() => fetchData(offset, true)}
                  disabled={loading}
                  className="px-6 py-2.5 border border-border rounded-xl text-sm font-medium text-ink-light hover:text-ink hover:border-ink transition-all disabled:opacity-50"
                >
                  {loading ? 'Chargement…' : 'Afficher 50 de plus'}
                </button>
              </div>
            )}
          </>
        )}
      </main>
      <footer className="border-t border-border px-6 py-6 text-center text-xs text-ink-light">
        VivreIci · BPE 2024 INSEE · DVF 2024 DGFiP · SSMSI 2024 · APL 2023 DREES · CEREMA 2023 · Filosofi 2021 INSEE ·{' '}
        <Link to="/methode" className="underline hover:text-ink">Méthode</Link>
      </footer>
    </div>
  )
}
