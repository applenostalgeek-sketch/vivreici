import { useParams, Link, useNavigate, useSearchParams } from 'react-router-dom'
import { useState, useEffect } from 'react'
import ScoreCard from '../components/ScoreCard.jsx'
import Nav from '../components/Nav.jsx'
import MapView from '../components/MapView.jsx'
import { CATEGORY_META, IRIS_CATEGORIES_LOCAL, IRIS_CATEGORIES_COMMUNE } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { scoreToColor, scoreToTextClass } from '../utils/scoreUtils.js'

const TYP_IRIS_LABEL = {
  H: 'Quartier résidentiel',
  A: "Zone d'activité",
  D: 'Zone diversifiée',
  Z: 'Commune entière',
}

export default function Iris() {
  const { codeIris } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const tab = searchParams.get('tab') || 'carte'
  const markerLat = searchParams.get('lat') ? parseFloat(searchParams.get('lat')) : null
  const markerLng = searchParams.get('lng') ? parseFloat(searchParams.get('lng')) : null

  function setTab(t) {
    setSearchParams(prev => { prev.set('tab', t); return prev })
  }

  usePageMeta({
    title: data ? `${data.nom} — Quartier ${data.score?.lettre || '?'}` : 'Quartier IRIS',
    description: data ? `Score du quartier ${data.nom} : ${data.score?.lettre || '?'} (${Math.round(data.score?.score_global || 0)}/100).` : null,
  })

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(`/data/iris/${codeIris}.json`)
      .then(r => {
        if (!r.ok) throw new Error(`IRIS ${codeIris} introuvable`)
        return r.json()
      })
      .then(d => {
        // IRIS de type Z = commune entière → rediriger vers la page commune
        if (d.typ_iris === 'Z') {
          navigate(`/commune/${d.code_commune}`, { replace: true })
          return
        }
        setData(d)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [codeIris])

  const codeCommune = data?.code_commune || codeIris?.slice(0, 5)

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
          <p className="text-2xl font-display text-ink mb-2">Zone IRIS introuvable</p>
          <p className="text-ink-light mb-6">{error}</p>
          <Link to="/" className="underline text-ink hover:text-ink-light">← Retour à l'accueil</Link>
        </div>
      )}

      {data && !loading && (
        <>
        <main className="max-w-4xl mx-auto px-6 py-12">
          {/* Breadcrumb */}
          <div className="flex items-center gap-2 text-sm text-ink-light mb-8">
            <Link to="/" className="hover:text-ink">Accueil</Link>
            <span>/</span>
            <button onClick={() => navigate(`/commune/${codeCommune}`)} className="hover:text-ink">
              {data.commune_nom || codeCommune}
            </button>
            <span>/</span>
            <span className="text-ink">{data.nom}</span>
          </div>

          {/* Onglets */}
          <div className="flex gap-1 bg-paper rounded-xl p-1 w-fit mb-8 border border-border">
            {['carte', 'detail'].map(t => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-all ${
                  tab === t ? 'bg-white shadow-sm text-ink' : 'text-ink-light hover:text-ink'
                }`}
              >
                {t === 'detail' ? 'Détail' : 'Carte'}
              </button>
            ))}
          </div>

          {/* Onglet CARTE */}
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
                  initialZoom={15}
                  marker={markerLat && markerLng ? { lat: markerLat, lng: markerLng } : null}
                  className="h-full"
                />
              </div>
            </div>
          )}

          {/* Onglet DETAIL */}
          {tab === 'detail' && (<>

          {/* Avertissement données partielles */}
          {data.score && data.score.nb_categories_scorees < 3 && (
            <div className="flex items-start gap-3 mb-8 bg-amber-50 border border-amber-200 rounded-xl px-5 py-3">
              <span className="text-amber-500 text-lg flex-shrink-0">⚠</span>
              <div>
                <div className="text-sm font-medium text-ink">Données partielles</div>
                <div className="text-xs text-ink-light mt-0.5">
                  Score basé sur {data.score.nb_categories_scorees} seule catégorie — insuffisant pour une lettre fiable.
                  Ce quartier n'est pas classé avec les autres.
                </div>
              </div>
            </div>
          )}

          {/* Rang dans la commune */}
          {data.rang_commune > 0 && data.nb_iris_commune > 0 && (
            <div className="flex items-center gap-3 mb-8 bg-paper border border-border rounded-xl px-5 py-3">
              <div className="text-center flex-shrink-0">
                <div className="font-mono text-2xl font-bold text-ink">#{data.rang_commune}</div>
                <div className="text-xs text-ink-light">sur {data.nb_iris_commune}</div>
              </div>
              <div className="w-px h-8 bg-border flex-shrink-0" />
              <div className="flex-1">
                <div className="text-sm font-medium text-ink mb-1">Rang dans la commune</div>
                <div className="w-full h-2 bg-border rounded-full overflow-hidden">
                  <div
                    className="h-full bg-score-A rounded-full transition-all"
                    style={{ width: `${Math.max(4, 100 - ((data.rang_commune - 1) / data.nb_iris_commune) * 100)}%` }}
                  />
                </div>
              </div>
              <div className="text-xs text-ink-light flex-shrink-0">
                {data.rang_commune === 1 ? 'Meilleur quartier'
                  : data.rang_commune <= data.nb_iris_commune * 0.25 ? 'Top 25%'
                  : data.rang_commune <= data.nb_iris_commune * 0.75 ? 'Dans la moyenne'
                  : 'En dessous de la moyenne'}
              </div>
            </div>
          )}

          {/* Header */}
          <div className="flex flex-col md:flex-row md:items-start justify-between gap-8 mb-12">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <span className="text-xs font-mono bg-paper border border-border rounded px-2 py-0.5 text-ink-light">IRIS</span>
                {data.typ_iris && (
                  <span className="text-xs text-ink-light">{TYP_IRIS_LABEL[data.typ_iris] || data.typ_iris}</span>
                )}
              </div>
              <h1 className="font-display text-4xl md:text-5xl text-ink mb-2">{data.nom}</h1>
              <div className="flex flex-wrap items-center gap-3 text-ink-light">
{data.population > 0 && (
                  <span className="font-medium text-ink">{data.population.toLocaleString('fr-FR')} hab.</span>
                )}
                <button
                  onClick={() => navigate(`/commune/${codeCommune}`)}
                  className="text-sm underline hover:text-ink"
                >
                  Voir la commune →
                </button>
              </div>
            </div>

            {data.score && data.score.lettre ? (
              <div className="flex-shrink-0">
                <ScoreCard lettre={data.score.lettre} score={data.score.score_global} size="lg" />
              </div>
            ) : data.score ? (
              <div className="bg-paper border border-border rounded-2xl px-6 py-4 text-center">
                <div className="font-mono text-3xl font-bold text-ink-light mb-1">?</div>
                <p className="text-ink-light text-sm">Données insuffisantes</p>
                <p className="text-xs text-ink-light mt-1">{Math.round(data.score.score_global)} pts · {data.score.nb_categories_scorees} catégorie</p>
              </div>
            ) : (
              <div className="bg-paper border border-border rounded-2xl px-6 py-4 text-center">
                <p className="text-ink-light text-sm">Score en cours de calcul</p>
              </div>
            )}
          </div>

          {data.score && (() => {
            const brutes = data.score.donnees_brutes || {}

            // ── Stat inline par catégorie ────────────────────────────────────
            function catStat(key) {
              if (key === 'sante') {
                if (brutes.apl_medecins > 0) return `${brutes.apl_medecins.toFixed(1)} consult./an/hab`
                if (brutes.medecins_pour_10000 > 0) return `${brutes.medecins_pour_10000.toFixed(1)} médecins/10 000 hab`
              }
              if (key === 'immobilier' && brutes.prix_m2_median > 0)
                return `${Math.round(brutes.prix_m2_median).toLocaleString('fr-FR')} €/m²`
              return null
            }

            // ── Rows helper ──────────────────────────────────────────────────
            function CatRow({ catKey }) {
              const meta = CATEGORY_META[catKey]
              const val = data.score.sous_scores?.[catKey]
              if (val == null) return null
              const pct = Math.round(val)
              const color = scoreToColor(pct)
              const textCls = scoreToTextClass(pct)
              const stat = catStat(catKey)
              return (
                <div className="flex items-center gap-3 py-3 first:pt-0 last:pb-0">
                  <span className="text-base w-5 flex-shrink-0 text-center">{meta.icon}</span>
                  <span className="text-sm font-medium text-ink w-24 sm:w-32 flex-shrink-0">{meta.label}</span>
                  <span className={`font-display font-bold text-base w-5 flex-shrink-0 ${textCls}`}>
                    {pct >= 80 ? 'A' : pct >= 60 ? 'B' : pct >= 40 ? 'C' : pct >= 20 ? 'D' : 'E'}
                  </span>
                  <div className="flex-1 h-5 bg-border rounded-full overflow-hidden">
                    <div className="h-full rounded-full" style={{ width: `${pct}%`, backgroundColor: color }} />
                  </div>
                  <span className="hidden sm:block text-xs text-ink-muted text-right w-44 flex-shrink-0 truncate">{stat}</span>
                </div>
              )
            }

            // ── POI Sur place ────────────────────────────────────────────────
            const detail = brutes.equipements_detail || {}
            const poi = brutes.poi_detail || {}
            const has = (bpe, p) => (bpe && detail[bpe] > 0) || (p && poi[p] > 0)
            const POI_GROUPES = [
              { label: 'Commerces', items: [
                { label: 'Boulangerie', bpe: 'boulangerie', poi: 'boulangerie' },
                { label: 'Boucherie',   bpe: 'boucherie',   poi: 'boucherie' },
                { label: 'Supermarché', bpe: 'supermarché', poi: 'supermarché' },
              ]},
              { label: 'Santé', items: [
                { label: 'Médecin', bpe: 'médecin_généraliste', poi: null },
                { label: 'Pharmacie', bpe: 'pharmacie', poi: null },
                { label: 'Hôpital',   bpe: 'hôpital',   poi: null },
              ]},
              { label: 'Éducation', items: [
                { label: 'Maternelle', bpe: 'école_maternelle',  poi: 'école_maternelle' },
                { label: 'Primaire',   bpe: 'école_élémentaire', poi: 'école_primaire' },
                { label: 'Collège',    bpe: 'collège',            poi: 'collège' },
                { label: 'Lycée',      bpe: 'lycée',              poi: 'lycée' },
              ]},
              { label: 'Sports', items: [
                { label: 'Piscine',  bpe: null,          poi: 'piscine' },
                { label: 'Gymnase',  bpe: 'gymnase',     poi: 'gymnase' },
                { label: 'Stade',    bpe: null,           poi: 'stade' },
              ]},
              { label: 'Culture', items: [
                { label: 'Cinéma',       bpe: 'cinéma',       poi: 'cinéma' },
                { label: 'Bibliothèque', bpe: 'bibliothèque', poi: 'bibliothèque' },
                { label: 'Théâtre',      bpe: 'théâtre',      poi: 'théâtre' },
                { label: 'Musée',        bpe: null,            poi: 'musée' },
              ]},
            ]
            const poiActifs = POI_GROUPES
              .map(g => ({ ...g, present: g.items.filter(it => has(it.bpe, it.poi)).map(it => it.label) }))
              .filter(g => g.present.length > 0)

            const hasCommune = IRIS_CATEGORIES_COMMUNE.some(k => data.score.sous_scores?.[k] != null)

            return (
              <div className="space-y-4">

                {/* Contexte commune */}
                {data.commune_score && (
                  <div className="flex items-center gap-3 bg-paper border border-border rounded-xl px-5 py-3">
                    <div className="flex-1 text-sm text-ink-light">
                      Score global de <strong className="text-ink">{data.commune_nom}</strong>
                    </div>
                    <div className="flex-shrink-0 flex items-center gap-2">
                      <span className="font-mono text-sm text-ink-light">{Math.round(data.commune_score.score_global)}</span>
                      <div className={`score-badge w-8 h-8 text-sm score-badge-${data.commune_score.lettre}`}>{data.commune_score.lettre}</div>
                    </div>
                  </div>
                )}

                {/* ── Catégories ── */}
                <div className="bg-white rounded-2xl border border-border p-6">
                  <div className="divide-y divide-border">
                    {IRIS_CATEGORIES_LOCAL.map(k => <CatRow key={k} catKey={k} />)}
                  </div>

                  {hasCommune && (
                    <>
                      <div className="flex items-center gap-3 my-4">
                        <div className="flex-1 h-px bg-border" />
                        <span className="text-xs text-ink-light uppercase tracking-wider flex-shrink-0">Données ville · {data.commune_nom}</span>
                        <div className="flex-1 h-px bg-border" />
                      </div>
                      <div className="divide-y divide-border">
                        {IRIS_CATEGORIES_COMMUNE.map(k => <CatRow key={k} catKey={k} />)}
                      </div>
                    </>
                  )}

                  {data.score.nb_categories_scorees < 6 && (
                    <p className="mt-4 text-xs text-ink-light border-t border-border pt-4">
                      Score calculé sur <strong className="text-ink">{data.score.nb_categories_scorees}</strong> catégorie(s) — même méthodologie que les communes.
                    </p>
                  )}
                </div>

                {/* ── Contexte (données non scorées) ── */}
                {(brutes.revenu_median > 0 || brutes.taux_pauvrete > 0) && (
                  <div className="grid grid-cols-3 divide-x divide-border bg-white rounded-2xl border border-border overflow-hidden">
                    {brutes.revenu_median > 0 && (
                      <div className="px-5 py-4">
                        <div className="font-mono text-lg font-semibold text-ink">
                          {Math.round(brutes.revenu_median).toLocaleString('fr-FR')} €
                        </div>
                        <div className="text-xs text-ink-muted mt-0.5">revenu médian / an</div>
                      </div>
                    )}
                    {brutes.prix_m2_median > 0 && brutes.revenu_median > 0 && (
                      <div className="px-5 py-4">
                        <div className="font-mono text-lg font-semibold text-ink">
                          {(brutes.prix_m2_median * 80 / brutes.revenu_median).toFixed(1)} ans
                        </div>
                        <div className="text-xs text-ink-muted mt-0.5">pour acheter 80 m²</div>
                      </div>
                    )}
                    {brutes.taux_pauvrete > 0 && (
                      <div className="px-5 py-4">
                        <div className="font-mono text-lg font-semibold text-ink">
                          {brutes.taux_pauvrete.toFixed(1)} %
                        </div>
                        <div className="text-xs text-ink-muted mt-0.5">taux de pauvreté</div>
                      </div>
                    )}
                  </div>
                )}

                {/* ── Sur place ── */}
                {poiActifs.length > 0 && (
                  <div className="bg-white rounded-2xl border border-border p-6">
                    <p className="text-xs font-semibold uppercase tracking-wider text-ink-muted mb-4">Sur place</p>
                    <div className="space-y-3">
                      {poiActifs.map(g => (
                        <div key={g.label} className="flex items-start gap-3">
                          <span className="text-xs text-ink-muted w-20 flex-shrink-0 pt-0.5">{g.label}</span>
                          <div className="flex flex-wrap gap-1.5">
                            {g.present.map(label => (
                              <span key={label} className="text-xs bg-paper text-ink-light border border-border px-2.5 py-1 rounded-full">
                                {label}
                              </span>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

              </div>
            )
          })()}

          </>)}

          {/* Actions */}
          <div className="mt-10 space-y-2">
            <button
              onClick={() => navigate(`/commune/${codeCommune}`)}
              className="w-full flex items-center justify-between bg-white border border-border rounded-xl px-5 py-4 hover:border-ink/40 transition-all group"
            >
              <span className="text-sm text-ink-light group-hover:text-ink">Voir tous les quartiers de cette commune</span>
              <span className="text-ink-light">→</span>
            </button>
          </div>

          {/* Méthode */}
          <div className="mt-10 pt-8 border-t border-border text-sm text-ink-light">
            <p>
              Les zones IRIS (INSEE) regroupent ~2 000 habitants chacune.
              Équipements, Santé et Prix au m² sont calculés à l'échelle du quartier.
              Sécurité, Transports et Éducation proviennent de la commune — même valeur pour tous ses quartiers.{' '}
              <Link to="/methode" className="underline hover:text-ink">En savoir plus sur la méthode</Link>
            </p>
          </div>
        </main>
        <footer className="border-t border-border px-6 py-5 text-center text-xs text-ink-light">
          lebonquartier · open data français · 2026 ·{' '}
          <Link to="/methode" className="underline hover:text-ink">Méthode</Link>
        </footer>
        </>
      )}
    </div>
  )
}
