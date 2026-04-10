import { useState, useEffect, useCallback } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { getClassement, getDeptClassement, getIrisClassement } from '../hooks/useSearch.js'
import Nav from '../components/Nav.jsx'
import { POP_OPTIONS } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { useProfile } from '../context/ProfileContext.jsx'
import { PROFILES } from '../utils/profiles.js'

const SCORE_TEXT = { A: 'text-score-A', B: 'text-score-B', C: 'text-score-C', D: 'text-score-D', E: 'text-score-E' }
const SCORE_BAR  = { A: 'bg-score-A',  B: 'bg-score-B',  C: 'bg-score-C',  D: 'bg-score-D',  E: 'bg-score-E' }

const TABS = [
  { key: 'departements', label: 'Départements' },
  { key: 'communes',     label: 'Communes' },
  { key: 'quartiers',    label: 'Quartiers' },
]

const DEPT_NAMES = {
  '01':'Ain','02':'Aisne','03':'Allier','04':'Alpes-de-Haute-Provence','05':'Hautes-Alpes',
  '06':'Alpes-Maritimes','07':'Ardèche','08':'Ardennes','09':'Ariège','10':'Aube',
  '11':'Aude','12':'Aveyron','13':'Bouches-du-Rhône','14':'Calvados','15':'Cantal',
  '16':'Charente','17':'Charente-Maritime','18':'Cher','19':'Corrèze','21':'Côte-d\'Or',
  '22':'Côtes-d\'Armor','23':'Creuse','24':'Dordogne','25':'Doubs','26':'Drôme',
  '27':'Eure','28':'Eure-et-Loir','29':'Finistère','2A':'Corse-du-Sud','2B':'Haute-Corse',
  '30':'Gard','31':'Haute-Garonne','32':'Gers','33':'Gironde','34':'Hérault',
  '35':'Ille-et-Vilaine','36':'Indre','37':'Indre-et-Loire','38':'Isère','39':'Jura',
  '40':'Landes','41':'Loir-et-Cher','42':'Loire','43':'Haute-Loire','44':'Loire-Atlantique',
  '45':'Loiret','46':'Lot','47':'Lot-et-Garonne','48':'Lozère','49':'Maine-et-Loire',
  '50':'Manche','51':'Marne','52':'Haute-Marne','53':'Mayenne','54':'Meurthe-et-Moselle',
  '55':'Meuse','56':'Morbihan','57':'Moselle','58':'Nièvre','59':'Nord',
  '60':'Oise','61':'Orne','62':'Pas-de-Calais','63':'Puy-de-Dôme','64':'Pyrénées-Atlantiques',
  '65':'Hautes-Pyrénées','66':'Pyrénées-Orientales','67':'Bas-Rhin','68':'Haut-Rhin','69':'Rhône',
  '70':'Haute-Saône','71':'Saône-et-Loire','72':'Sarthe','73':'Savoie','74':'Haute-Savoie',
  '75':'Paris','76':'Seine-Maritime','77':'Seine-et-Marne','78':'Yvelines','79':'Deux-Sèvres',
  '80':'Somme','81':'Tarn','82':'Tarn-et-Garonne','83':'Var','84':'Vaucluse',
  '85':'Vendée','86':'Vienne','87':'Haute-Vienne','88':'Vosges','89':'Yonne',
  '90':'Territoire de Belfort','91':'Essonne','92':'Hauts-de-Seine','93':'Seine-Saint-Denis',
  '94':'Val-de-Marne','95':'Val-d\'Oise','971':'Guadeloupe','972':'Martinique',
  '973':'Guyane','974':'La Réunion','976':'Mayotte',
}

function ScoreBadge({ lettre, score, small = false }) {
  return (
    <div className="flex items-center gap-2 flex-shrink-0">
      <div className="hidden sm:block">
        <div className={`w-16 h-1.5 bg-paper rounded-full overflow-hidden`}>
          <div className={`h-full rounded-full ${SCORE_BAR[lettre]}`} style={{ width: `${score}%` }} />
        </div>
      </div>
      <span className={`font-mono text-sm font-semibold tabular-nums ${SCORE_TEXT[lettre]}`}>
        {Math.round(score)}
      </span>
      <div className={`score-badge ${small ? 'w-7 h-7 text-xs' : 'w-8 h-8 text-sm'} flex-shrink-0 score-badge-${lettre}`}>
        {lettre}
      </div>
    </div>
  )
}

function Spinner() {
  return (
    <div className="flex items-center justify-center py-20 text-ink-light gap-3">
      <div className="w-5 h-5 border-2 border-border border-t-ink rounded-full animate-spin" />
      Chargement…
    </div>
  )
}

function Empty() {
  return (
    <div className="text-center py-20 text-ink-light">
      <p className="text-lg font-display text-ink mb-2">Aucun résultat</p>
      <p className="text-sm">Essayez un autre filtre ou département.</p>
    </div>
  )
}

// ── Départements tab ────────────────────────────────────────────────────────

function DeptTab({ weights, onSelectDept }) {
  const [depts, setDepts] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    getDeptClassement({ weights })
      .then(setDepts)
      .catch(() => setDepts([]))
      .finally(() => setLoading(false))
  }, [weights])

  if (loading) return <Spinner />
  if (depts.length === 0) return <Empty />

  return (
    <>
      <p className="text-xs text-ink-light mb-4 font-mono">{depts.length} départements</p>
      <div className="space-y-1.5">
        {depts.map((d, i) => (
          <button
            key={d.dept}
            onClick={() => onSelectDept(d.dept)}
            className="w-full bg-white border border-border rounded-xl px-4 py-3 hover:border-ink/40 hover:shadow-sm transition-all duration-150 text-left group"
          >
            <div className="flex items-center gap-3">
              <span className="font-mono text-sm text-border w-7 text-right flex-shrink-0">{i + 1}</span>
              <div className="flex-1 min-w-0">
                <div className="flex items-baseline gap-2">
                  <span className="font-semibold text-ink group-hover:underline">{DEPT_NAMES[d.dept] || d.dept}</span>
                  <span className="text-xs font-mono text-ink-light">{d.dept}</span>
                </div>
                <div className="flex items-center gap-3 mt-0.5">
                  <span className="text-xs text-ink-light">{d.nb_communes} communes</span>
                  <span className="text-xs text-ink-light">{d.population.toLocaleString('fr-FR')} hab.</span>
                </div>
              </div>
              <ScoreBadge lettre={d.lettre} score={d.score_moy} />
            </div>
          </button>
        ))}
      </div>
    </>
  )
}

// ── Communes tab ────────────────────────────────────────────────────────────

function CommunesTab({ dept, setDept, weights }) {
  const navigate = useNavigate()
  const [communes, setCommunes] = useState([])
  const [loading, setLoading] = useState(true)
  const [minPop, setMinPop] = useState(2000)
  const [deptInput, setDeptInput] = useState(dept)
  const [hasMore, setHasMore] = useState(false)
  const [offset, setOffset] = useState(0)
  const LIMIT = 50

  const fetchData = useCallback(async (newOffset = 0, append = false) => {
    setLoading(true)
    try {
      const params = { limit: LIMIT, ordre: 'desc', min_population: minPop, offset: newOffset }
      if (dept) params.departement = dept
      if (weights) params.weights = weights
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
  }, [minPop, dept, weights])

  useEffect(() => {
    setOffset(0)
    fetchData(0, false)
  }, [fetchData])

  // Sync deptInput when dept changes from outside (e.g. dept tab click)
  useEffect(() => { setDeptInput(dept) }, [dept])

  const handleDeptSubmit = (e) => {
    e.preventDefault()
    setDept(deptInput.trim().toUpperCase())
  }

  return (
    <>
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3 mb-5">
        {/* Population */}
        <div className="flex flex-wrap gap-1.5">
          {POP_OPTIONS.map(opt => (
            <button
              key={opt.value}
              onClick={() => setMinPop(opt.value)}
              className={`px-2.5 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                minPop === opt.value
                  ? 'bg-ink text-white border-ink'
                  : 'border-border text-ink-light hover:border-ink hover:text-ink'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>

        <span className="text-border">|</span>

        {/* Département */}
        <form onSubmit={handleDeptSubmit} className="flex items-center gap-1.5">
          <input
            type="text"
            value={deptInput}
            onChange={e => setDeptInput(e.target.value)}
            placeholder="Dept…"
            maxLength={3}
            className="w-16 border border-border rounded-lg px-2 py-1.5 text-xs font-mono focus:outline-none focus:border-ink"
          />
          <button type="submit" className="px-2.5 py-1.5 text-xs bg-ink text-white rounded-lg hover:bg-ink/80 transition-colors">
            OK
          </button>
          {dept && (
            <button type="button" onClick={() => { setDept(''); setDeptInput('') }} className="text-xs text-ink-light hover:text-ink underline">
              Tous
            </button>
          )}
        </form>

        {dept && (
          <span className="text-xs font-mono text-ink bg-paper border border-border rounded px-2 py-0.5">
            {DEPT_NAMES[dept] || dept}
          </span>
        )}
      </div>

      {loading && communes.length === 0 ? <Spinner /> : communes.length === 0 ? <Empty /> : (
        <>
          <p className="text-xs text-ink-light mb-3 font-mono">
            {communes.length}{hasMore ? '+' : ''} commune{communes.length > 1 ? 's' : ''}
            {dept && ` · ${DEPT_NAMES[dept] || dept}`}
          </p>

          <div className="space-y-1.5">
            {communes.map((c, i) => {
              const s = c.score
              const lettre = s?.lettre || 'C'
              const score = s?.score_global || 0
              return (
                <button
                  key={c.code_insee}
                  onClick={() => navigate(`/commune/${c.code_insee}`)}
                  className="w-full bg-white border border-border rounded-xl px-4 py-3 hover:border-ink/40 hover:shadow-sm transition-all duration-150 text-left group"
                >
                  <div className="flex items-center gap-3">
                    <span className="font-mono text-sm text-border w-7 text-right flex-shrink-0">{i + 1}</span>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-baseline gap-2 flex-wrap">
                        <span className="font-semibold text-ink group-hover:underline truncate">{c.nom}</span>
                        <span className="text-xs font-mono text-ink-light">{c.departement}</span>
                        {c.population > 0 && (
                          <span className="text-xs text-ink-light hidden sm:block">
                            {c.population.toLocaleString('fr-FR')} hab.
                          </span>
                        )}
                      </div>
                    </div>
                    <ScoreBadge lettre={lettre} score={score} />
                  </div>
                </button>
              )
            })}
          </div>

          {hasMore && (
            <div className="mt-5 text-center">
              <button
                onClick={() => fetchData(offset, true)}
                disabled={loading}
                className="px-5 py-2 border border-border rounded-xl text-sm font-medium text-ink-light hover:text-ink hover:border-ink transition-all disabled:opacity-50"
              >
                {loading ? 'Chargement…' : 'Afficher 50 de plus'}
              </button>
            </div>
          )}
        </>
      )}
    </>
  )
}

// ── Quartiers tab ───────────────────────────────────────────────────────────

function QuartiersTab({ dept, setDept }) {
  const navigate = useNavigate()
  const [items, setItems] = useState([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [offset, setOffset] = useState(0)
  const [deptInput, setDeptInput] = useState(dept)
  const LIMIT = 50

  const fetchData = useCallback(async (newOffset = 0, append = false) => {
    setLoading(true)
    try {
      const { total: t, items: data } = await getIrisClassement({ departement: dept, limit: LIMIT, offset: newOffset })
      if (append) {
        setItems(prev => [...prev, ...data])
      } else {
        setItems(data)
      }
      setTotal(t)
      setOffset(newOffset + data.length)
    } catch {
      if (!append) { setItems([]); setTotal(0) }
    } finally {
      setLoading(false)
    }
  }, [dept])

  useEffect(() => {
    if (!dept) { setItems([]); setTotal(0); return }
    setOffset(0)
    fetchData(0, false)
  }, [fetchData, dept])

  useEffect(() => { setDeptInput(dept) }, [dept])

  const handleDeptSubmit = (e) => {
    e.preventDefault()
    setDept(deptInput.trim().toUpperCase())
  }

  return (
    <>
      {/* Dept filter */}
      <div className="flex flex-wrap items-center gap-3 mb-5">
        <form onSubmit={handleDeptSubmit} className="flex items-center gap-1.5">
          <input
            type="text"
            value={deptInput}
            onChange={e => setDeptInput(e.target.value)}
            placeholder="Département…"
            maxLength={3}
            className="w-28 border border-border rounded-lg px-2.5 py-1.5 text-xs font-mono focus:outline-none focus:border-ink"
          />
          <button type="submit" className="px-2.5 py-1.5 text-xs bg-ink text-white rounded-lg hover:bg-ink/80 transition-colors">
            OK
          </button>
          {dept && (
            <button type="button" onClick={() => { setDept(''); setDeptInput('') }} className="text-xs text-ink-light hover:text-ink underline">
              Tous
            </button>
          )}
        </form>
        {dept && (
          <span className="text-xs font-mono text-ink bg-paper border border-border rounded px-2 py-0.5">
            {DEPT_NAMES[dept] || dept}
          </span>
        )}
      </div>

      {!dept ? (
        <div className="text-center py-16 text-ink-light">
          <p className="text-sm">Sélectionnez un département pour voir les quartiers.</p>
        </div>
      ) : loading && items.length === 0 ? <Spinner /> : items.length === 0 ? <Empty /> : (
        <>
          <p className="text-xs text-ink-light mb-3 font-mono">
            {total} quartier{total > 1 ? 's' : ''} · {DEPT_NAMES[dept] || dept}
          </p>

          <div className="space-y-1.5">
            {items.map((q, i) => (
              <button
                key={q.code_iris}
                onClick={() => navigate(`/iris/${q.code_iris}`)}
                className="w-full bg-white border border-border rounded-xl px-4 py-3 hover:border-ink/40 hover:shadow-sm transition-all duration-150 text-left group"
              >
                <div className="flex items-center gap-3">
                  <span className="font-mono text-sm text-border w-7 text-right flex-shrink-0">{i + 1}</span>
                  <div className="flex-1 min-w-0">
                    <span className="font-semibold text-ink group-hover:underline truncate block">{q.nom}</span>
                    <span className="text-xs text-ink-light">{q.commune_nom}</span>
                  </div>
                  <ScoreBadge lettre={q.lettre} score={q.score_global} small />
                </div>
              </button>
            ))}
          </div>

          {items.length < total && (
            <div className="mt-5 text-center">
              <button
                onClick={() => fetchData(offset, true)}
                disabled={loading}
                className="px-5 py-2 border border-border rounded-xl text-sm font-medium text-ink-light hover:text-ink hover:border-ink transition-all disabled:opacity-50"
              >
                {loading ? 'Chargement…' : 'Afficher 50 de plus'}
              </button>
            </div>
          )}
        </>
      )}
    </>
  )
}

// ── Main page ───────────────────────────────────────────────────────────────

export default function Classement() {
  usePageMeta({
    title: 'Classement national',
    description: 'Classement des communes françaises par score de qualité de vie. Filtrez par département, taille et critères.',
  })

  const [searchParams] = useSearchParams()
  const [tab, setTab] = useState(() => searchParams.get('vue') || 'departements')
  const [dept, setDept] = useState(() => searchParams.get('departement') || '')
  const navigate = useNavigate()
  const { profile } = useProfile()
  const weights = profile !== 'national' ? PROFILES[profile]?.weights : null

  const handleSelectDept = (d) => {
    setDept(d)
    setTab('communes')
  }

  return (
    <div className="min-h-screen bg-paper">
      <Nav />

      <main className="max-w-3xl mx-auto px-4 sm:px-6 py-8 sm:py-10">
        {/* Header */}
        <div className="mb-6">
          <div className="flex items-center gap-3 flex-wrap">
            <h1 className="font-display text-3xl sm:text-4xl text-ink">Classement</h1>
            {profile !== 'national' && (
              <span className="flex items-center gap-1.5 px-3 py-1 rounded-full bg-ink text-paper text-sm font-medium">
                {PROFILES[profile].emoji} {PROFILES[profile].label}
              </span>
            )}
          </div>
        </div>

        {/* Tabs */}
        <div className="flex gap-1 bg-white border border-border rounded-xl p-1 mb-6">
          {TABS.map(t => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`flex-1 py-2 rounded-lg text-sm font-medium transition-all ${
                tab === t.key
                  ? 'bg-ink text-white shadow-sm'
                  : 'text-ink-light hover:text-ink'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* Content */}
        {tab === 'departements' && (
          <DeptTab weights={weights} onSelectDept={handleSelectDept} />
        )}
        {tab === 'communes' && (
          <CommunesTab dept={dept} setDept={setDept} weights={weights} />
        )}
        {tab === 'quartiers' && (
          <QuartiersTab dept={dept} setDept={setDept} />
        )}
      </main>

      <footer className="border-t border-border px-6 py-5 text-center text-xs text-ink-light">
        lebonquartier · open data français · 2026 ·{' '}
        <Link to="/a-propos" className="underline hover:text-ink">À propos</Link>
      </footer>
    </div>
  )
}
