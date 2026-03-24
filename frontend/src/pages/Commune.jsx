import { useParams, Link, useNavigate, useSearchParams } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { getCommune, getClassement } from '../hooks/useSearch.js'
import ScoreCard from '../components/ScoreCard.jsx'
import CategoryBreakdown from '../components/CategoryBreakdown.jsx'
import Nav from '../components/Nav.jsx'
import ScoreBar from '../components/ScoreBar.jsx'
import MapView from '../components/MapView.jsx'
import { CATEGORY_META } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'

export default function Commune() {
  const { codeInsee } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()
  const tab = searchParams.get('tab') || 'carte'
  const markerLat = searchParams.get('lat') ? parseFloat(searchParams.get('lat')) : null
  const markerLng = searchParams.get('lng') ? parseFloat(searchParams.get('lng')) : null

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [voisines, setVoisines] = useState([])
  const navigate = useNavigate()

  function setTab(t) {
    setSearchParams(prev => { const p = new URLSearchParams(prev); p.set('tab', t); return p })
  }

  usePageMeta({
    title: data ? `${data.nom} — ${data.score?.lettre || '?'} · ${Math.round(data.score?.score_global || 0)}/100` : 'Commune',
    description: data ? `Score de qualité de vie de ${data.nom} (${data.departement}) : ${data.score?.lettre || '?'} (${Math.round(data.score?.score_global || 0)}/100). Équipements, sécurité, santé, éducation, transports.` : null,
  })

  useEffect(() => {
    setLoading(true)
    setError(null)
    setVoisines([])
    getCommune(codeInsee)
      .then(d => {
        setData(d)
        if (d.departement) {
          getClassement({ departement: d.departement, limit: 6, sort: 'score', ordre: 'desc', min_population: 0 })
            .then(v => setVoisines(v.filter(c => c.code_insee !== codeInsee).slice(0, 5)))
            .catch(() => {})
        }
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [codeInsee])

  return (
    <div className="min-h-screen">
      <Nav searchPlaceholder="Autre commune…" />

      {loading && (
        <div className="flex items-center justify-center py-32 text-ink-light">
          <div className="w-6 h-6 border-2 border-border border-t-ink rounded-full animate-spin mr-3" />
          Chargement…
        </div>
      )}

      {error && (
        <div className="max-w-lg mx-auto mt-20 text-center">
          <p className="text-2xl font-display text-ink mb-2">Commune introuvable</p>
          <p className="text-ink-light mb-6">{error}</p>
          <Link to="/" className="underline text-ink hover:text-ink-light">← Retour à l'accueil</Link>
        </div>
      )}

      {data && !loading && (
        <main className="max-w-4xl mx-auto px-6 py-12">
          {/* Tabs CARTE / DETAIL */}
          <div className="flex items-center gap-1 mb-8 bg-paper border border-border rounded-xl p-1 w-fit">
            <button
              onClick={() => setTab('carte')}
              className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-all ${
                tab === 'carte'
                  ? 'bg-white text-ink shadow-sm border border-border'
                  : 'text-ink-light hover:text-ink'
              }`}
            >
              Carte
            </button>
            <button
              onClick={() => setTab('detail')}
              className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-all ${
                tab === 'detail'
                  ? 'bg-white text-ink shadow-sm border border-border'
                  : 'text-ink-light hover:text-ink'
              }`}
            >
              Détail
            </button>
          </div>

          {/* ── TAB CARTE ──────────────────────────────────────────────── */}
          {tab === 'carte' && data.latitude && data.longitude && (
            <div>
              <div className="flex justify-end mb-2 print:hidden">
                <button onClick={() => window.print()} className="px-3 py-1.5 text-sm border border-border rounded-lg text-ink-light hover:text-ink hover:border-ink transition-all">
                  Imprimer
                </button>
              </div>
              <div className="rounded-2xl overflow-hidden border border-border" style={{ height: '70vh' }}>
                <MapView
                  initialCenter={markerLat && markerLng ? [markerLat, markerLng] : [data.latitude, data.longitude]}
                  initialZoom={markerLat ? 15 : 13}
                  marker={markerLat && markerLng ? { lat: markerLat, lng: markerLng } : null}
                  className="h-full"
                />
              </div>
            </div>
          )}

          {/* ── TAB DETAIL ─────────────────────────────────────────────── */}
          {tab === 'detail' && (<>
          {/* Breadcrumb */}
          <div className="flex items-center gap-2 text-sm text-ink-light mb-8">
            <Link to="/" className="hover:text-ink">Accueil</Link>
            <span>/</span>
            {data.departement && (
              <>
                <button onClick={() => navigate(`/classement?departement=${data.departement}`)} className="hover:text-ink">
                  Dept. {data.departement}
                </button>
                <span>/</span>
              </>
            )}
            <span className="text-ink">{data.nom}</span>
          </div>

          {/* Header */}
          <div className="flex flex-col md:flex-row md:items-start justify-between gap-8 mb-12">
            <div className="flex-1 min-w-0">
              <div className="flex items-start justify-between gap-4 mb-2">
                <h1 className="font-display text-4xl md:text-5xl text-ink">{data.nom}</h1>
                <button
                  onClick={() => window.print()}
                  className="px-3 py-1.5 text-sm border border-border rounded-lg text-ink-light hover:text-ink hover:border-ink transition-all print:hidden flex-shrink-0"
                >
                  Imprimer
                </button>
              </div>
              <div className="flex flex-wrap items-center gap-3 text-ink-light">
{data.departement && <span>{data.departement}</span>}
                {data.region && <><span>·</span><span>{data.region}</span></>}
                {data.population > 0 && (
                  <><span>·</span><span className="font-medium text-ink">{data.population.toLocaleString('fr-FR')} hab.</span></>
                )}
                {data.codes_postaux?.length > 0 && (
                  <span className="text-xs font-mono">{data.codes_postaux[0]}</span>
                )}
                {data.rang_departement && data.nb_communes_departement && (
                  <>
                    <span>·</span>
                    <span>
                      {data.rang_departement === 1
                        ? <strong className="text-ink">1ère commune du département</strong>
                        : <><strong className="text-ink">{data.rang_departement}ème</strong> / {data.nb_communes_departement} communes</>}
                    </span>
                  </>
                )}
              </div>
            </div>

            {data.score ? (
              <div className="flex-shrink-0">
                <ScoreCard lettre={data.score.lettre} score={data.score.score_global} size="lg" />
              </div>
            ) : (
              <div className="bg-paper border border-border rounded-2xl px-6 py-4 text-center">
                <p className="text-ink-light text-sm">Score en cours de calcul</p>
              </div>
            )}
          </div>

          {data.score && (
            <div className="space-y-6">


              {/* Score bars */}
              <div className="bg-white rounded-2xl border border-border p-6">
                <h2 className="font-display text-xl text-ink mb-6">Scores par catégorie</h2>
                <div className="space-y-5">
                  {Object.entries(CATEGORY_META).map(([key, meta]) => {
                    const val = data.score.sous_scores[key]
                    if (val == null) return null
                    return <ScoreBar key={key} value={val} label={meta.label} icon={meta.icon} desc={meta.desc} />
                  })}
                </div>
                {data.score.nb_categories_scorees < 6 && (
                  <p className="mt-6 text-xs text-ink-light border-t border-border pt-4">
                    Score calculé sur <strong className="text-ink">{data.score.nb_categories_scorees}</strong> catégorie(s).
                    Les données manquantes seront ajoutées prochainement.
                  </p>
                )}
              </div>

              {/* Données brutes */}
              <div className="bg-white rounded-2xl border border-border p-6">
                <h2 className="font-display text-xl text-ink mb-4">Données brutes</h2>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                  {data.score.donnees_brutes.nb_equipements > 0 && (
                    <div className="bg-paper rounded-xl p-4">
                      <div className="font-mono text-xl font-bold text-ink">{data.score.donnees_brutes.nb_equipements}</div>
                      <div className="text-xs text-ink-light mt-1">équipements recensés</div>
                      <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">BPE 2024 INSEE</div>
                    </div>
                  )}
                  {data.score.donnees_brutes.apl_medecins > 0 && (
                    <div className="bg-paper rounded-xl p-4">
                      <div className="font-mono text-xl font-bold text-ink">{data.score.donnees_brutes.apl_medecins.toFixed(2)}</div>
                      <div className="text-xs text-ink-light mt-1">consultations/an/hab. (APL)</div>
                      <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">APL 2023 DREES</div>
                    </div>
                  )}
                  {data.score.donnees_brutes.taux_criminalite > 0 && (
                    <div className="bg-paper rounded-xl p-4">
                      <div className="font-mono text-xl font-bold text-ink">{data.score.donnees_brutes.taux_criminalite.toFixed(1)}</div>
                      <div className="text-xs text-ink-light mt-1">délits / 1 000 hab.</div>
                      <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">SSMSI 2024</div>
                    </div>
                  )}
                  {data.score.donnees_brutes.prix_m2_median > 0 && (() => {
                    const p2024 = data.score.donnees_brutes.prix_m2_median
                    const p2022 = data.score.donnees_brutes.prix_m2_median_2022
                    const hasTrend = p2022 && p2022 > 0
                    const pctChange = hasTrend ? ((p2024 - p2022) / p2022 * 100) : null
                    const trendUp = pctChange > 2
                    const trendDown = pctChange < -2
                    return (
                      <div className="bg-paper rounded-xl p-4">
                        <div className="flex items-start justify-between">
                          <div className="font-mono text-xl font-bold text-ink">
                            {Math.round(p2024).toLocaleString('fr-FR')} €
                          </div>
                          {pctChange != null && (
                            <span className={`text-xs font-mono font-semibold ${trendUp ? 'text-score-D' : trendDown ? 'text-score-B' : 'text-ink-light'}`}>
                              {trendUp ? '↑' : trendDown ? '↓' : '→'} {pctChange > 0 ? '+' : ''}{Math.round(pctChange)}%
                            </span>
                          )}
                        </div>
                        <div className="text-xs text-ink-light mt-1">
                          prix médian au m²{hasTrend ? ' (vs 2022)' : ''}
                        </div>
                        <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">DVF 2024 DGFiP</div>
                      </div>
                    )
                  })()}
                  {data.score.donnees_brutes.revenu_median > 0 && (
                    <div className="bg-paper rounded-xl p-4">
                      <div className="font-mono text-xl font-bold text-ink">
                        {Math.round(data.score.donnees_brutes.revenu_median).toLocaleString('fr-FR')} €
                      </div>
                      <div className="text-xs text-ink-light mt-1">revenu médian / an (info)</div>
                      <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">Filosofi 2021 INSEE</div>
                    </div>
                  )}
                  {data.score.donnees_brutes.taux_pauvrete > 0 && (
                    <div className="bg-paper rounded-xl p-4">
                      <div className="font-mono text-xl font-bold text-ink">
                        {data.score.donnees_brutes.taux_pauvrete.toFixed(1)} %
                      </div>
                      <div className="text-xs text-ink-light mt-1">taux de pauvreté</div>
                      <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">Filosofi 2021 INSEE</div>
                    </div>
                  )}
                  {data.score.donnees_brutes.distance_gare_km != null && (
                    <div className="bg-paper rounded-xl p-4">
                      <div className="font-mono text-xl font-bold text-ink">
                        {data.score.donnees_brutes.distance_gare_km < 1
                          ? `< 1 km`
                          : `${data.score.donnees_brutes.distance_gare_km} km`}
                      </div>
                      <div className="text-xs text-ink-light mt-1">
                        {data.score.donnees_brutes.nb_gares > 0
                          ? 'gare voyageurs sur place'
                          : 'gare voyageurs la plus proche'}
                      </div>
                      <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">SNCF data.gouv.fr</div>
                    </div>
                  )}
                </div>

              </div>

              {/* Équipements présents — sources officielles (FINESS, Annuaire Édu, RES, OSM) */}
              {data.score.donnees_brutes.poi_detail && (() => {
                const poi = data.score.donnees_brutes.poi_detail
                const GROUPES = [
                  { label: 'Commerces', icon: '🛒', keys: [
                    { key: 'boulangerie', label: 'Boulangerie' },
                    { key: 'boucherie', label: 'Boucherie' },
                    { key: 'supermarché', label: 'Supermarché' },
                  ]},
                  { label: 'Santé', icon: '🏥', keys: [
                    { key: 'pharmacie', label: 'Pharmacie' },
                    { key: 'cabinet_médical', label: 'Cabinet médical' },
                    { key: 'hôpital', label: 'Hôpital' },
                    { key: 'clinique', label: 'Clinique' },
                    { key: 'labo_analyse', label: 'Laboratoire' },
                  ]},
                  { label: 'Éducation', icon: '🎓', keys: [
                    { key: 'école_maternelle', label: 'Maternelle' },
                    { key: 'école_primaire', label: 'Primaire' },
                    { key: 'collège', label: 'Collège' },
                    { key: 'lycée', label: 'Lycée' },
                    { key: 'lycée_professionnel', label: 'Lycée pro' },
                  ]},
                  { label: 'Sports', icon: '⚽', keys: [
                    { key: 'piscine', label: 'Piscine' },
                    { key: 'gymnase', label: 'Gymnase' },
                    { key: 'stade', label: 'Stade' },
                  ]},
                  { label: 'Culture', icon: '🎭', keys: [
                    { key: 'cinéma', label: 'Cinéma' },
                    { key: 'bibliothèque', label: 'Bibliothèque' },
                    { key: 'théâtre', label: 'Théâtre' },
                    { key: 'musée', label: 'Musée' },
                  ]},
                ]
                const groupesActifs = GROUPES
                  .map(g => ({ ...g, present: g.keys.filter(({ key }) => (poi[key] || 0) > 0).map(({ label }) => label) }))
                  .filter(g => g.present.length > 0)
                if (!groupesActifs.length) return null
                return (
                  <div className="bg-white rounded-2xl border border-border p-6">
                    <h2 className="font-display text-xl text-ink mb-4">Équipements</h2>
                    <div className="space-y-2">
                      {groupesActifs.map(g => (
                        <div key={g.label} className="flex items-start gap-2">
                          <span className="text-sm flex-shrink-0 mt-0.5">{g.icon}</span>
                          <div className="flex flex-wrap gap-1">
                            {g.present.map(label => (
                              <span key={label} className="text-xs bg-surface-alt text-ink-light px-2 py-0.5 rounded-full border border-border">
                                {label}
                              </span>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                    <p className="text-xs text-ink-muted mt-4">Sources : FINESS, Annuaire éducation, RES, OSM</p>
                  </div>
                )
              })()}

            </div>
          )}

          {/* Quartiers IRIS */}
          {data.iris && data.iris.length > 0 && (() => {
            const complete = data.iris.filter(z => !z.donnees_partielles)
            const partial = data.iris.filter(z => z.donnees_partielles)
            const scoreMin = complete[complete.length - 1]?.lettre
            const scoreMax = complete[0]?.lettre
            return (
              <div className="mt-10">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h2 className="font-display text-xl text-ink">Scores par quartier</h2>
                    <p className="text-sm text-ink-light mt-0.5">
                      {complete.length} quartiers scorés{partial.length > 0 ? ` · ${partial.length} données partielles` : ''}{scoreMin && scoreMax ? ` · Score ${scoreMin} → ${scoreMax}` : ''}
                    </p>
                  </div>
                  <span className="text-xs text-ink-light bg-paper border border-border rounded-full px-3 py-1">
                    Meilleur en premier
                  </span>
                </div>

                <div className="space-y-2">
                  {complete.map((iris, i) => {
                    const label = i === 0 ? 'Meilleur quartier'
                      : i < complete.length * 0.25 ? 'Top 25%'
                      : i < complete.length * 0.75 ? 'Quartier moyen'
                      : 'Quartier en dessous'
                    const labelColor = i === 0 ? 'text-score-A'
                      : i < complete.length * 0.25 ? 'text-score-B'
                      : i < complete.length * 0.75 ? 'text-score-C'
                      : 'text-score-D'
                    return (
                      <button
                        key={iris.code_iris}
                        onClick={() => navigate(`/iris/${iris.code_iris}?tab=detail`)}
                        className="w-full flex items-center gap-3 bg-white border border-border rounded-xl px-4 py-3 hover:border-ink/40 transition-all text-left group"
                      >
                        <span className="font-mono text-sm text-ink-light w-6 flex-shrink-0 text-center">{i + 1}</span>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-ink group-hover:underline truncate">{iris.nom}</span>
                            <span className={`text-xs flex-shrink-0 ${labelColor}`}>{label}</span>
                          </div>
                          {iris.population > 0 && (
                            <span className="text-xs text-ink-light">{iris.population.toLocaleString('fr-FR')} hab.</span>
                          )}
                        </div>
                        {iris.score_global != null ? (
                          <div className="flex items-center gap-2 flex-shrink-0">
                            <span className="font-mono text-sm text-ink-light">{Math.round(iris.score_global)}</span>
                            <div className={`score-badge w-8 h-8 text-sm score-badge-${iris.lettre}`}>{iris.lettre}</div>
                          </div>
                        ) : (
                          <span className="text-xs text-ink-light flex-shrink-0">—</span>
                        )}
                      </button>
                    )
                  })}
                </div>
                <div className="mt-4">
                  <button
                    onClick={() => navigate(`/commune/${data.code_insee}/quartiers`)}
                    className="text-sm text-ink-light underline hover:text-ink"
                  >
                    Voir le classement complet des quartiers →
                  </button>
                </div>
              </div>
            )
          })()}

          {/* Top du département */}
          {voisines.length > 0 && (
            <div className="mt-10">
              <h2 className="font-display text-xl text-ink mb-4">
                Top communes — Département {data.departement}
              </h2>
              <div className="space-y-2">
                {voisines.map(c => (
                  <button
                    key={c.code_insee}
                    onClick={() => navigate(`/commune/${c.code_insee}?tab=detail`)}
                    className="w-full flex items-center gap-4 bg-white border border-border rounded-xl px-5 py-3 hover:border-ink/40 transition-all text-left group"
                  >
                    <div className="flex-1 min-w-0">
                      <span className="font-medium text-ink group-hover:underline">{c.nom}</span>
                      {c.population > 0 && (
                        <span className="ml-2 text-xs text-ink-light">{c.population.toLocaleString('fr-FR')} hab.</span>
                      )}
                    </div>
                    {c.score && (
                      <div className="flex items-center gap-2 flex-shrink-0">
                        <span className="font-mono text-sm text-ink-light">{Math.round(c.score.score_global)}</span>
                        <div className={`score-badge w-8 h-8 text-sm score-badge-${c.score.lettre}`}>{c.score.lettre}</div>
                      </div>
                    )}
                  </button>
                ))}
              </div>
              <div className="mt-3">
                <button
                  onClick={() => navigate(`/classement?departement=${data.departement}`)}
                  className="text-sm text-ink-light underline hover:text-ink"
                >
                  Voir tout le département {data.departement} →
                </button>
              </div>
            </div>
          )}

          {/* Méthode + disclaimer */}
          <div className="mt-10 pt-8 border-t border-border space-y-3 text-sm text-ink-light">
            <p>
              Score calculé à partir de données open data françaises (BPE 2024 INSEE, DVF 2024 DGFiP, SSMSI 2024, APL 2023 DREES, IPS/DNB DEPP, Filosofi 2021 INSEE).
              Les scores sont des percentiles nationaux : 50 = médiane nationale, 80 = top 20%.{' '}
              <Link to="/methode" className="underline hover:text-ink">En savoir plus sur la méthode</Link>
            </p>
            <p className="text-xs border border-border/60 rounded-lg px-4 py-3 bg-paper">
              Ce score est un outil de comparaison objective basé sur des données publiques. Il ne remplace pas le conseil d'un professionnel local (agent immobilier, notaire) qui connaît le terrain et les projets d'urbanisme en cours.
            </p>
          </div>
          </>)} {/* fin tab detail */}
        </main>
      )}
    </div>
  )
}
