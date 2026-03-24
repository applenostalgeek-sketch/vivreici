import { useState, useEffect } from 'react'
import { useSearchParams, Link, useNavigate } from 'react-router-dom'
import Nav from '../components/Nav.jsx'
import SearchBar from '../components/SearchBar.jsx'
import { SCORE_COLORS } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'

const CAT_META = [
  { key: 'equipements', label: 'Équipements',          color: '#16A34A' },
  { key: 'sante',       label: 'Santé',                color: '#0EA5E9' },
  { key: 'immobilier',  label: 'Accessibilité logement',color: '#8B5CF6' },
]

/**
 * Génère une synthèse comparative en langage naturel.
 */
function genererSynthese(d1, d2) {
  if (!d1?.score || !d2?.score) return null
  const s1 = d1.score.score_global ?? 0
  const s2 = d2.score.score_global ?? 0
  const n1 = d1.nom
  const n2 = d2.nom
  const sous1 = d1.score.sous_scores || {}
  const sous2 = d2.score.sous_scores || {}

  const delta = Math.round(Math.abs(s1 - s2))
  const gagnant = s1 > s2 ? n1 : s2 > s1 ? n2 : null
  const perdant = s1 > s2 ? n2 : s2 > s1 ? n1 : null

  // Avantages catégorie par catégorie
  const avantages1 = []
  const avantages2 = []
  for (const cat of CAT_META) {
    const v1 = sous1[cat.key]
    const v2 = sous2[cat.key]
    if (v1 == null || v2 == null) continue
    const diff = Math.round(v1 - v2)
    if (diff >= 10) avantages1.push({ label: cat.label.toLowerCase(), diff })
    if (diff <= -10) avantages2.push({ label: cat.label.toLowerCase(), diff: -diff })
  }
  avantages1.sort((a, b) => b.diff - a.diff)
  avantages2.sort((a, b) => b.diff - a.diff)

  const parts = []

  // Verdict global
  if (!gagnant) {
    parts.push(`**${n1}** et **${n2}** ont un score global quasi identique.`)
  } else if (delta < 5) {
    parts.push(`**${gagnant}** devance légèrement **${perdant}** (+${delta} pts au global).`)
  } else {
    parts.push(`**${gagnant}** domine clairement **${perdant}** (+${delta} pts au global).`)
  }

  // Avantages
  if (avantages1.length > 0) {
    const cats = avantages1.slice(0, 2).map(a => `${a.label} (+${a.diff} pts)`).join(' et ')
    parts.push(`**${n1}** est meilleur sur : ${cats}.`)
  }
  if (avantages2.length > 0) {
    const cats = avantages2.slice(0, 2).map(a => `${a.label} (+${a.diff} pts)`).join(' et ')
    parts.push(`**${n2}** est meilleur sur : ${cats}.`)
  }

  // Recommandation contextuelle
  const immo1 = sous1.immobilier ?? 0
  const immo2 = sous2.immobilier ?? 0
  const edu1 = sous1.education ?? sous1.revenus ?? 0
  const edu2 = sous2.education ?? sous2.revenus ?? 0
  const sante1 = sous1.sante ?? 0
  const sante2 = sous2.sante ?? 0

  if (avantages1.some(a => a.label === 'accessibilité logement') || avantages2.some(a => a.label === 'accessibilité logement')) {
    const accessible = immo1 > immo2 ? n1 : n2
    parts.push(`Pour un budget logement serré, **${accessible}** est plus accessible.`)
  }
  if (Math.abs(sante1 - sante2) >= 10) {
    const meilleurSante = sante1 > sante2 ? n1 : n2
    parts.push(`Pour un accès facilité aux soins, **${meilleurSante}** est recommandé.`)
  }

  return parts
}

function ScorePill({ lettre, score }) {
  if (!lettre) return <div className="w-16 h-16 rounded-2xl bg-paper border border-border flex items-center justify-center text-ink-light">—</div>
  return (
    <div
      className="w-16 h-16 rounded-2xl flex flex-col items-center justify-center text-white shadow-sm"
      style={{ backgroundColor: SCORE_COLORS[lettre] || '#9CA3AF' }}
    >
      <span className="font-display font-bold text-2xl leading-none">{lettre}</span>
      <span className="text-xs opacity-80">{Math.round(score)}</span>
    </div>
  )
}

function CompareBar({ val1, val2, label, color }) {
  const p1 = val1 != null && val1 >= 0 ? Math.round(val1) : null
  const p2 = val2 != null && val2 >= 0 ? Math.round(val2) : null
  const winner = p1 != null && p2 != null ? (p1 > p2 ? 1 : p2 > p1 ? 2 : 0) : 0

  return (
    <div className="grid grid-cols-[1fr_80px_1fr] gap-3 items-center py-2">
      {/* Left bar */}
      <div className="flex items-center gap-2 justify-end">
        {p1 != null ? (
          <>
            <span className={`text-sm font-mono ${winner === 1 ? 'font-bold text-ink' : 'text-ink-light'}`}>{p1}</span>
            <div className="w-24 h-2 bg-paper rounded-full overflow-hidden flex justify-end">
              <div className="h-full rounded-full" style={{ width: `${p1}%`, backgroundColor: winner === 1 ? color : '#D1D5DB' }} />
            </div>
          </>
        ) : <span className="text-xs text-ink-light">—</span>}
      </div>

      {/* Label */}
      <div className="text-center">
        <span className="text-xs font-medium text-ink-light uppercase tracking-wider">{label}</span>
      </div>

      {/* Right bar */}
      <div className="flex items-center gap-2">
        {p2 != null ? (
          <>
            <div className="w-24 h-2 bg-paper rounded-full overflow-hidden">
              <div className="h-full rounded-full" style={{ width: `${p2}%`, backgroundColor: winner === 2 ? color : '#D1D5DB' }} />
            </div>
            <span className={`text-sm font-mono ${winner === 2 ? 'font-bold text-ink' : 'text-ink-light'}`}>{p2}</span>
          </>
        ) : <span className="text-xs text-ink-light">—</span>}
      </div>
    </div>
  )
}

function IrisPicker({ onPick }) {
  const [step, setStep] = useState('commune') // 'commune' | 'iris'
  const [communeName, setCommuneName] = useState('')
  const [communeCode, setCommuneCode] = useState(null)
  const [irisList, setIrisList] = useState([])
  const [loadingIris, setLoadingIris] = useState(false)

  function handleCommuneSelect(commune) {
    setCommuneCode(commune.code_insee)
    setCommuneName(commune.nom)
    setLoadingIris(true)
    fetch(`/data/communes/${commune.code_insee}.json`)
      .then(r => r.json())
      .then(data => {
        setIrisList((data.iris || []).filter(z => z.score_global != null).slice(0, 30))
        setStep('iris')
      })
      .catch(() => {})
      .finally(() => setLoadingIris(false))
  }

  if (step === 'commune') {
    return (
      <div className="h-full flex flex-col gap-4">
        <p className="text-sm text-ink-light">Choisissez une commune :</p>
        <SearchBar size="sm" placeholder="Commune ou adresse…" onSelect={handleCommuneSelect} />
        {loadingIris && <p className="text-sm text-ink-light">Chargement des quartiers…</p>}
      </div>
    )
  }

  return (
    <div className="h-full flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <p className="text-sm text-ink-light">Quartiers de <strong className="text-ink">{communeName}</strong></p>
        <button onClick={() => setStep('commune')} className="text-xs text-ink-light underline hover:text-ink">Changer</button>
      </div>
      <div className="space-y-1.5 max-h-96 overflow-y-auto">
        {irisList.map((z, i) => (
          <button
            key={z.code_iris}
            onClick={() => onPick(z.code_iris)}
            className="w-full flex items-center gap-3 bg-white border border-border rounded-xl px-4 py-2.5 hover:border-ink/40 transition-all text-left"
          >
            <span className="font-mono text-xs text-ink-light w-5">{i + 1}</span>
            <span className="flex-1 text-sm font-medium text-ink truncate">{z.nom}</span>
            {z.lettre && (
              <div
                className="w-7 h-7 rounded-lg flex items-center justify-center text-white text-xs font-bold flex-shrink-0"
                style={{ backgroundColor: SCORE_COLORS[z.lettre] }}
              >
                {z.lettre}
              </div>
            )}
          </button>
        ))}
      </div>
    </div>
  )
}

export default function CompareIris() {
  usePageMeta({
    title: 'Comparer deux quartiers',
    description: 'Comparaison de deux quartiers IRIS — scores détaillés par catégorie.',
  })

  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const c1 = searchParams.get('c1')
  const c2 = searchParams.get('c2')

  const [data1, setData1] = useState(null)
  const [data2, setData2] = useState(null)

  useEffect(() => {
    if (!c1) return
    fetch(`/data/iris/${c1}.json`).then(r => r.json()).then(setData1).catch(() => {})
  }, [c1])

  useEffect(() => {
    if (!c2) { setData2(null); return }
    fetch(`/data/iris/${c2}.json`).then(r => r.json()).then(setData2).catch(() => {})
  }, [c2])

  function pickC2(code) {
    setSearchParams({ c1, c2: code })
  }

  if (!c1) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center gap-4 text-ink-light">
        <p>Accédez à cette page depuis une fiche quartier.</p>
        <Link to="/" className="underline hover:text-ink">Accueil</Link>
      </div>
    )
  }

  const sous1 = data1?.score?.sous_scores || {}
  const sous2 = data2?.score?.sous_scores || {}

  return (
    <div className="min-h-screen">
      <Nav searchBar={false}>
        <span className="text-sm font-medium text-ink-light">Comparaison de quartiers</span>
      </Nav>

      <main className="max-w-4xl mx-auto px-6 py-10">
        <h1 className="font-display text-3xl text-ink mb-8">Comparer deux quartiers</h1>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Column 1 */}
          <div className="bg-white border border-border rounded-2xl p-6">
            {data1 ? (
              <>
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h2 className="font-display text-xl text-ink">{data1.nom}</h2>
                    <p className="text-xs text-ink-light mt-0.5">
                      {data1.code_iris}
                      {data1.rang_commune && <> · #{data1.rang_commune}/{data1.nb_iris_commune} dans la commune</>}
                    </p>
                  </div>
                  <ScorePill lettre={data1.score?.lettre} score={data1.score?.score_global} />
                </div>
                <button
                  onClick={() => navigate(`/iris/${c1}`)}
                  className="text-xs text-ink-light underline hover:text-ink"
                >
                  Voir la fiche →
                </button>
              </>
            ) : (
              <div className="h-24 flex items-center justify-center">
                <div className="w-5 h-5 border-2 border-border border-t-ink rounded-full animate-spin" />
              </div>
            )}
          </div>

          {/* Column 2 */}
          <div className="bg-white border border-border rounded-2xl p-6">
            {data2 ? (
              <>
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h2 className="font-display text-xl text-ink">{data2.nom}</h2>
                    <p className="text-xs text-ink-light mt-0.5">
                      {data2.code_iris}
                      {data2.rang_commune && <> · #{data2.rang_commune}/{data2.nb_iris_commune} dans la commune</>}
                    </p>
                  </div>
                  <ScorePill lettre={data2.score?.lettre} score={data2.score?.score_global} />
                </div>
                <button
                  onClick={() => setSearchParams({ c1 })}
                  className="text-xs text-ink-light underline hover:text-ink"
                >
                  Changer →
                </button>
              </>
            ) : (
              <IrisPicker onPick={pickC2} />
            )}
          </div>
        </div>

        {/* Comparison bars — only when both loaded */}
        {data1 && data2 && (
          <div className="mt-8 bg-white border border-border rounded-2xl p-6">
            <h2 className="font-display text-xl text-ink mb-4">Comparaison par catégorie</h2>

            {/* Column labels */}
            <div className="grid grid-cols-[1fr_80px_1fr] gap-3 mb-2">
              <p className="text-sm font-semibold text-ink text-right truncate pr-2">{data1.nom}</p>
              <span />
              <p className="text-sm font-semibold text-ink truncate pl-2">{data2.nom}</p>
            </div>

            {CAT_META.map(cat => (
              <CompareBar
                key={cat.key}
                val1={sous1[cat.key]}
                val2={sous2[cat.key]}
                label={cat.label}
                color={cat.color}
              />
            ))}

            {/* Global score bar */}
            <div className="mt-3 pt-3 border-t border-border">
              <CompareBar
                val1={data1.score?.score_global}
                val2={data2.score?.score_global}
                label="Global"
                color="#374151"
              />
            </div>
          </div>
        )}

        {/* Synthèse langage naturel */}
        {data1 && data2 && (() => {
          const phrases = genererSynthese(data1, data2)
          if (!phrases || phrases.length === 0) return null
          return (
            <div className="mt-4 bg-paper border border-border rounded-2xl px-6 py-5">
              <div className="flex items-center gap-2 mb-3">
                <span className="text-sm font-semibold text-ink">Synthèse</span>
              </div>
              <ul className="space-y-2">
                {phrases.map((p, i) => (
                  <li key={i} className="text-sm text-ink-light leading-relaxed"
                    dangerouslySetInnerHTML={{
                      __html: p.replace(/\*\*(.+?)\*\*/g, '<strong class="text-ink">$1</strong>')
                    }}
                  />
                ))}
              </ul>
            </div>
          )
        })()}

        {/* Raw data comparison */}
        {data1 && data2 && (
          <div className="mt-4 bg-white border border-border rounded-2xl p-6">
            <h2 className="font-display text-lg text-ink mb-4">Données brutes</h2>
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div className="font-medium text-ink-light text-right pr-4">
                {data1.score?.donnees_brutes?.nb_equipements > 0 && <div className="py-1">{data1.score.donnees_brutes.nb_equipements}</div>}
                {data1.score?.donnees_brutes?.prix_m2_median > 0 && <div className="py-1">{Math.round(data1.score.donnees_brutes.prix_m2_median).toLocaleString('fr-FR')} €</div>}
                {data1.score?.donnees_brutes?.revenu_median > 0 && <div className="py-1">{Math.round(data1.score.donnees_brutes.revenu_median).toLocaleString('fr-FR')} €</div>}
              </div>
              <div className="text-ink-light text-center">
                {data1.score?.donnees_brutes?.nb_equipements > 0 && <div className="py-1 text-xs">équipements</div>}
                {data1.score?.donnees_brutes?.prix_m2_median > 0 && <div className="py-1 text-xs">prix médian m²</div>}
                {data1.score?.donnees_brutes?.revenu_median > 0 && <div className="py-1 text-xs">revenu médian</div>}
              </div>
              <div className="font-medium text-ink pl-4">
                {data2.score?.donnees_brutes?.nb_equipements > 0 && <div className="py-1">{data2.score.donnees_brutes.nb_equipements}</div>}
                {data2.score?.donnees_brutes?.prix_m2_median > 0 && <div className="py-1">{Math.round(data2.score.donnees_brutes.prix_m2_median).toLocaleString('fr-FR')} €</div>}
                {data2.score?.donnees_brutes?.revenu_median > 0 && <div className="py-1">{Math.round(data2.score.donnees_brutes.revenu_median).toLocaleString('fr-FR')} €</div>}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
