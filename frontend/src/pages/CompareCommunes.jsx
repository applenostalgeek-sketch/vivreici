import { useState, useEffect } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import Nav from '../components/Nav.jsx'
import { CATEGORY_META } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'

const SCORE_TEXT = { A: 'text-score-A', B: 'text-score-B', C: 'text-score-C', D: 'text-score-D', E: 'text-score-E' }

function ScoreDelta({ v1, v2 }) {
  if (v1 == null || v2 == null) return <span className="text-xs text-ink-light">—</span>
  const d = Math.round(v1 - v2)
  if (Math.abs(d) < 2) return <span className="text-xs text-ink-light font-mono">=</span>
  return (
    <span className={`text-xs font-mono font-semibold ${d > 0 ? 'text-score-A' : 'text-score-E'}`}>
      {d > 0 ? `+${d}` : d}
    </span>
  )
}

function CommuneSearch({ label, value, onChange }) {
  const [q, setQ] = useState('')
  const [results, setResults] = useState([])
  const [open, setOpen] = useState(false)

  useEffect(() => {
    if (q.length < 2) { setResults([]); return }
    const t = setTimeout(async () => {
      try {
        const r = await fetch(`/api/communes/search?q=${encodeURIComponent(q)}&limit=6`)
        if (r.ok) setResults(await r.json())
      } catch { setResults([]) }
    }, 250)
    return () => clearTimeout(t)
  }, [q])

  const select = (c) => {
    onChange(c)
    setQ('')
    setResults([])
    setOpen(false)
  }

  return (
    <div className="relative">
      {value ? (
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-ink">{value.nom}</span>
          <button onClick={() => { onChange(null); setOpen(true) }} className="text-xs text-ink-light underline hover:text-ink">
            changer
          </button>
        </div>
      ) : (
        <input
          autoFocus={open}
          value={q}
          onChange={e => { setQ(e.target.value); setOpen(true) }}
          placeholder={label}
          className="w-full border border-border rounded-xl px-4 py-2.5 text-sm focus:outline-none focus:border-ink"
        />
      )}
      {results.length > 0 && open && (
        <div className="absolute top-full left-0 right-0 z-10 mt-1 bg-white border border-border rounded-xl shadow-lg overflow-hidden">
          {results.map(c => (
            <button
              key={c.code_insee}
              onClick={() => select(c)}
              className="w-full text-left px-4 py-2.5 text-sm hover:bg-paper flex items-center justify-between"
            >
              <span className="font-medium text-ink">{c.nom}</span>
              <span className="text-xs text-ink-light font-mono">{c.departement}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

export default function CompareCommunes() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [commune1, setCommune1] = useState(null)
  const [commune2, setCommune2] = useState(null)
  const [data1, setData1] = useState(null)
  const [data2, setData2] = useState(null)

  usePageMeta({ title: 'Comparer deux communes' })

  // Charger depuis URL params au démarrage
  useEffect(() => {
    const c1 = searchParams.get('c1')
    const c2 = searchParams.get('c2')
    if (c1) fetch(`/api/communes/${c1}`).then(r => r.ok ? r.json() : null).then(d => { if (d) setCommune1({ code_insee: d.code_insee, nom: d.nom, departement: d.departement }) }).catch(() => {})
    if (c2) fetch(`/api/communes/${c2}`).then(r => r.ok ? r.json() : null).then(d => { if (d) setCommune2({ code_insee: d.code_insee, nom: d.nom, departement: d.departement }) }).catch(() => {})
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Fetch données complètes quand commune sélectionnée
  useEffect(() => {
    if (!commune1) { setData1(null); return }
    fetch(`/api/communes/${commune1.code_insee}`).then(r => r.ok ? r.json() : null).then(setData1)
    setSearchParams(p => { const np = new URLSearchParams(p); np.set('c1', commune1.code_insee); return np })
  }, [commune1])

  useEffect(() => {
    if (!commune2) { setData2(null); return }
    fetch(`/api/communes/${commune2.code_insee}`).then(r => r.ok ? r.json() : null).then(setData2)
    setSearchParams(p => { const np = new URLSearchParams(p); np.set('c2', commune2.code_insee); return np })
  }, [commune2])

  const s1 = data1?.score?.sous_scores || {}
  const s2 = data2?.score?.sous_scores || {}
  const brutes1 = data1?.score?.donnees_brutes || {}
  const brutes2 = data2?.score?.donnees_brutes || {}

  return (
    <div className="min-h-screen bg-paper">
      <Nav searchPlaceholder="Commune…" />
      <main className="max-w-4xl mx-auto px-6 py-10">

        <div className="mb-8">
          <h1 className="font-display text-4xl text-ink mb-1">Comparer deux communes</h1>
          <p className="text-ink-light text-sm">Scores et données brutes côte à côte</p>
        </div>

        {/* Sélecteurs */}
        <div className="grid grid-cols-2 gap-4 mb-8">
          <div className="bg-white border border-border rounded-2xl p-4">
            <p className="text-xs font-medium text-ink-light uppercase tracking-wider mb-3">Commune 1</p>
            <CommuneSearch label="Chercher une commune…" value={commune1} onChange={setCommune1} />
          </div>
          <div className="bg-white border border-border rounded-2xl p-4">
            <p className="text-xs font-medium text-ink-light uppercase tracking-wider mb-3">Commune 2</p>
            <CommuneSearch label="Chercher une commune…" value={commune2} onChange={setCommune2} />
          </div>
        </div>

        {/* Résultats */}
        {(data1 || data2) && (
          <div className="space-y-4">

            {/* Scores globaux */}
            <div className="bg-white border border-border rounded-2xl p-6">
              <div className="grid grid-cols-[1fr_3rem_1fr] gap-4 items-center">
                {/* Commune 1 */}
                <div>
                  {data1 ? (
                    <>
                      <Link to={`/commune/${data1.code_insee}`} className="font-display text-2xl text-ink hover:underline block">{data1.nom}</Link>
                      <p className="text-sm text-ink-light">{data1.departement} · {data1.population?.toLocaleString('fr-FR')} hab.</p>
                      {data1.score && (
                        <div className="mt-3 flex items-center gap-3">
                          <div className={`score-badge w-12 h-12 text-xl score-badge-${data1.score.lettre}`}>{data1.score.lettre}</div>
                          <span className="font-mono text-2xl font-bold text-ink">{Math.round(data1.score.score_global)}</span>
                        </div>
                      )}
                    </>
                  ) : <div className="h-16 flex items-center text-sm text-ink-light">—</div>}
                </div>
                {/* VS */}
                <div className="text-center font-display text-ink-light text-lg">vs</div>
                {/* Commune 2 */}
                <div className="text-right">
                  {data2 ? (
                    <>
                      <Link to={`/commune/${data2.code_insee}`} className="font-display text-2xl text-ink hover:underline block">{data2.nom}</Link>
                      <p className="text-sm text-ink-light">{data2.departement} · {data2.population?.toLocaleString('fr-FR')} hab.</p>
                      {data2.score && (
                        <div className="mt-3 flex items-center gap-3 justify-end">
                          <span className="font-mono text-2xl font-bold text-ink">{Math.round(data2.score.score_global)}</span>
                          <div className={`score-badge w-12 h-12 text-xl score-badge-${data2.score.lettre}`}>{data2.score.lettre}</div>
                        </div>
                      )}
                    </>
                  ) : <div className="h-16 flex items-center justify-end text-sm text-ink-light">—</div>}
                </div>
              </div>
            </div>

            {/* Scores par catégorie */}
            <div className="bg-white border border-border rounded-2xl p-6">
              <h2 className="font-display text-lg text-ink mb-5">Scores par catégorie</h2>
              <div className="space-y-3">
                {Object.entries(CATEGORY_META).map(([key, meta]) => {
                  const v1 = s1[key]
                  const v2 = s2[key]
                  if (v1 == null && v2 == null) return null
                  const p1 = v1 != null ? Math.round(v1) : null
                  const p2 = v2 != null ? Math.round(v2) : null
                  const l1 = p1 != null ? (p1 >= 80 ? 'A' : p1 >= 60 ? 'B' : p1 >= 40 ? 'C' : p1 >= 20 ? 'D' : 'E') : null
                  const l2 = p2 != null ? (p2 >= 80 ? 'A' : p2 >= 60 ? 'B' : p2 >= 40 ? 'C' : p2 >= 20 ? 'D' : 'E') : null
                  return (
                    <div key={key} className="grid grid-cols-[1fr_3rem_1fr] gap-3 items-center">
                      <div className="flex items-center gap-2 justify-end">
                        {p1 != null ? (
                          <>
                            <div className="flex-1 h-2 bg-paper rounded-full overflow-hidden max-w-24 ml-auto">
                              <div className={`h-full rounded-full score-badge-${l1}`} style={{ width: `${p1}%` }} />
                            </div>
                            <span className={`font-mono text-sm font-semibold w-8 text-right ${SCORE_TEXT[l1]}`}>{p1}</span>
                          </>
                        ) : <span className="text-xs text-ink-light">—</span>}
                      </div>
                      <div className="flex flex-col items-center gap-0.5">
                        <span className="text-base">{meta.icon}</span>
                        <ScoreDelta v1={v1} v2={v2} />
                      </div>
                      <div className="flex items-center gap-2">
                        {p2 != null ? (
                          <>
                            <span className={`font-mono text-sm font-semibold w-8 ${SCORE_TEXT[l2]}`}>{p2}</span>
                            <div className="flex-1 h-2 bg-paper rounded-full overflow-hidden max-w-24">
                              <div className={`h-full rounded-full score-badge-${l2}`} style={{ width: `${p2}%` }} />
                            </div>
                          </>
                        ) : <span className="text-xs text-ink-light">—</span>}
                      </div>
                    </div>
                  )
                })}
              </div>
              <div className="mt-4 pt-4 border-t border-border grid grid-cols-[1fr_3rem_1fr] gap-3 text-xs text-ink-light text-center">
                <span className="text-right">{data1?.nom}</span>
                <span></span>
                <span>{data2?.nom}</span>
              </div>
            </div>

            {/* Données brutes */}
            {(data1 || data2) && (
              <div className="bg-white border border-border rounded-2xl p-6">
                <h2 className="font-display text-lg text-ink mb-5">Données brutes</h2>
                <div className="space-y-2">
                  {[
                    { label: 'Prix médian au m²', v1: brutes1.prix_m2_median ? `${Math.round(brutes1.prix_m2_median).toLocaleString('fr-FR')} €` : null, v2: brutes2.prix_m2_median ? `${Math.round(brutes2.prix_m2_median).toLocaleString('fr-FR')} €` : null },
                    { label: 'APL médecins (consult./an/hab.)', v1: brutes1.apl_medecins ? brutes1.apl_medecins.toFixed(2) : null, v2: brutes2.apl_medecins ? brutes2.apl_medecins.toFixed(2) : null },
                    { label: 'Criminalité (délits/1 000 hab.)', v1: brutes1.taux_criminalite ? brutes1.taux_criminalite.toFixed(1) : null, v2: brutes2.taux_criminalite ? brutes2.taux_criminalite.toFixed(1) : null },
                    { label: 'Distance gare la plus proche', v1: brutes1.distance_gare_km != null ? (brutes1.distance_gare_km < 1 ? '< 1 km' : `${brutes1.distance_gare_km} km`) : null, v2: brutes2.distance_gare_km != null ? (brutes2.distance_gare_km < 1 ? '< 1 km' : `${brutes2.distance_gare_km} km`) : null },
                    { label: 'Revenu médian (€/an)', v1: brutes1.revenu_median ? Math.round(brutes1.revenu_median).toLocaleString('fr-FR') + ' €' : null, v2: brutes2.revenu_median ? Math.round(brutes2.revenu_median).toLocaleString('fr-FR') + ' €' : null },
                    { label: 'Taux de pauvreté', v1: brutes1.taux_pauvrete ? `${brutes1.taux_pauvrete.toFixed(1)} %` : null, v2: brutes2.taux_pauvrete ? `${brutes2.taux_pauvrete.toFixed(1)} %` : null },
                  ].filter(r => r.v1 != null || r.v2 != null).map(({ label, v1, v2 }) => (
                    <div key={label} className="grid grid-cols-[1fr_auto_1fr] gap-4 py-2 border-b border-border/50 last:border-0">
                      <span className="font-mono text-sm text-ink text-right">{v1 || '—'}</span>
                      <span className="text-xs text-ink-light text-center min-w-40">{label}</span>
                      <span className="font-mono text-sm text-ink">{v2 || '—'}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

          </div>
        )}

        {!commune1 && !commune2 && (
          <div className="text-center py-20 text-ink-light">
            <p className="font-display text-xl text-ink mb-2">Choisissez deux communes</p>
            <p className="text-sm">Recherchez une commune dans chaque champ ci-dessus pour comparer leurs scores.</p>
          </div>
        )}

      </main>
    </div>
  )
}
