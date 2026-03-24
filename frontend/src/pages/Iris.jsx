import { useParams, Link, useNavigate, useSearchParams } from 'react-router-dom'
import { useState, useEffect } from 'react'
import ScoreCard from '../components/ScoreCard.jsx'
import Nav from '../components/Nav.jsx'
import ScoreBar from '../components/ScoreBar.jsx'
import MapView from '../components/MapView.jsx'
import { CATEGORY_META, IRIS_CATEGORIES } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'

const TYP_IRIS_LABEL = {
  H: 'Quartier résidentiel',
  A: "Zone d'activité",
  D: 'Zone diversifiée',
  Z: 'Commune entière',
}

// Sous-ensemble des catégories disponibles au niveau IRIS
const IRIS_CAT_META = Object.fromEntries(IRIS_CATEGORIES.map(k => [k, CATEGORY_META[k]]))

export default function Iris() {
  const { codeIris } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const tab = searchParams.get('tab') || 'carte'
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
    fetch(`/api/iris/${codeIris}`)
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
                  initialCenter={[data.latitude, data.longitude]}
                  initialZoom={15}
                  className="h-full"
                />
              </div>
            </div>
          )}

          {/* Onglet DETAIL */}
          {tab === 'detail' && (<>

          {/* Avertissement données partielles */}
          {data.score && data.score.nb_categories_scorees < 2 && (
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

          {data.score && (
            <div className="space-y-6">
              {/* Score bars */}
              <div className="bg-white rounded-2xl border border-border p-6">
                <h2 className="font-display text-xl text-ink mb-6">Scores par catégorie</h2>
                <div className="space-y-5">
                  {Object.entries(IRIS_CAT_META).map(([key, meta]) => {
                    const val = data.score.sous_scores?.[key]
                    if (val == null) return null
                    return <ScoreBar key={key} value={val} label={meta.label} icon={meta.icon} desc={meta.desc} />
                  })}
                </div>
                {data.score.nb_categories_scorees < 4 && (
                  <p className="mt-6 text-xs text-ink-light border-t border-border pt-4">
                    Score calculé sur <strong className="text-ink">{data.score.nb_categories_scorees}</strong> catégorie(s).
                    Les IRIS ont moins de données disponibles que les communes.
                  </p>
                )}
              </div>

              {/* Données brutes */}
              {data.score.donnees_brutes && (
                <div className="bg-white rounded-2xl border border-border p-6">
                  <h2 className="font-display text-xl text-ink mb-4">Données brutes</h2>

                  {/* Équipements — fusion BPE (présence dans l'IRIS) + POI (GPS matching) */}
                  {(() => {
                    const detail = data.score.donnees_brutes.equipements_detail || {}
                    const poi = data.score.donnees_brutes.poi_detail || {}
                    const has = (bpe, p) => (bpe && detail[bpe] > 0) || (p && poi[p] > 0)
                    const GROUPES = [
                      { label: 'Commerces', icon: '🛒', items: [
                        { label: 'Boulangerie',  bpe: 'boulangerie',       poi: 'boulangerie' },
                        { label: 'Boucherie',    bpe: 'boucherie',         poi: 'boucherie' },
                        { label: 'Supermarché',  bpe: 'supermarché',       poi: 'supermarché' },
                        { label: 'Hypermarché',  bpe: 'hypermarché',       poi: null },
                      ]},
                      { label: 'Santé', icon: '🏥', items: [
                        { label: 'Médecin généraliste', bpe: 'médecin_généraliste', poi: null },
                        { label: 'Médecin spécialiste', bpe: 'médecin_spécialiste', poi: null },
                        { label: 'Pharmacie',           bpe: 'pharmacie',           poi: null },
                        { label: 'Hôpital',             bpe: 'hôpital',             poi: null },
                        { label: 'Urgences',            bpe: 'urgences',            poi: null },
                      ]},
                      { label: 'Éducation', icon: '🎓', items: [
                        { label: 'Maternelle', bpe: 'école_maternelle',   poi: 'école_maternelle' },
                        { label: 'Primaire',   bpe: 'école_élémentaire',  poi: 'école_primaire' },
                        { label: 'Collège',    bpe: 'collège',             poi: 'collège' },
                        { label: 'Lycée',      bpe: 'lycée',               poi: 'lycée' },
                        { label: 'Lycée pro',  bpe: 'lycée_professionnel', poi: 'lycée_professionnel' },
                      ]},
                      { label: 'Services', icon: '🏛️', items: [
                        { label: 'Mairie',          bpe: 'mairie',          poi: null },
                        { label: 'Bureau de poste', bpe: 'bureau_poste',    poi: null },
                        { label: 'Banque',          bpe: 'agence_bancaire', poi: null },
                      ]},
                      { label: 'Sports', icon: '⚽', items: [
                        { label: 'Piscine',        bpe: null,         poi: 'piscine' },
                        { label: 'Gymnase',        bpe: 'gymnase',    poi: 'gymnase' },
                        { label: 'Stade',          bpe: null,         poi: 'stade' },
                        { label: 'Salle de sport', bpe: 'salle_sport', poi: null },
                      ]},
                      { label: 'Culture', icon: '🎭', items: [
                        { label: 'Cinéma',       bpe: 'cinéma',       poi: 'cinéma' },
                        { label: 'Bibliothèque', bpe: 'bibliothèque', poi: 'bibliothèque' },
                        { label: 'Théâtre',      bpe: 'théâtre',      poi: 'théâtre' },
                        { label: 'Musée',        bpe: null,           poi: 'musée' },
                      ]},
                    ]
                    const groupesActifs = GROUPES
                      .map(g => ({ ...g, present: g.items.filter(it => has(it.bpe, it.poi)).map(it => it.label) }))
                      .filter(g => g.present.length > 0)
                    if (!groupesActifs.length) return null
                    return (
                      <div className="mb-5 pb-5 border-b border-border">
                        <h3 className="text-sm font-semibold text-ink mb-3">Équipements</h3>
                        <div className="space-y-2">
                          {groupesActifs.map(g => (
                            <div key={g.label} className="flex items-start gap-2">
                              <span className="text-sm flex-shrink-0 mt-0.5">{g.icon}</span>
                              <div className="flex flex-wrap gap-1">
                                {g.present.map(label => (
                                  <span key={label} className="text-xs bg-paper text-ink-light px-2 py-0.5 rounded-full border border-border">
                                    {label}
                                  </span>
                                ))}
                              </div>
                            </div>
                          ))}
                        </div>
                        <p className="text-xs text-ink-muted mt-3">BPE 2024 INSEE · Annuaire éducation · RES · OSM</p>
                      </div>
                    )
                  })()}

                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                    {data.score.donnees_brutes.nb_equipements > 0 && (
                      <div className="bg-paper rounded-xl p-4">
                        <div className="font-mono text-xl font-bold text-ink">{data.score.donnees_brutes.nb_equipements}</div>
                        <div className="text-xs text-ink-light mt-1">équipements recensés</div>
                        <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">BPE 2024 INSEE</div>
                      </div>
                    )}
                    {data.score.donnees_brutes.medecins_pour_10000 > 0 && (
                      <div className="bg-paper rounded-xl p-4">
                        <div className="font-mono text-xl font-bold text-ink">{data.score.donnees_brutes.medecins_pour_10000.toFixed(1)}</div>
                        <div className="text-xs text-ink-light mt-1">médecins / 10 000 hab.</div>
                        <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">BPE 2024 INSEE</div>
                      </div>
                    )}
                    {data.score.donnees_brutes.prix_m2_median > 0 && (
                      <div className="bg-paper rounded-xl p-4">
                        <div className="font-mono text-xl font-bold text-ink">
                          {Math.round(data.score.donnees_brutes.prix_m2_median).toLocaleString('fr-FR')} €
                        </div>
                        <div className="text-xs text-ink-light mt-1">prix médian au m²</div>
                        <div className="text-xs font-mono text-ink-muted mt-0.5 opacity-60">DVF 2024 DGFiP</div>
                      </div>
                    )}
                    {data.score.donnees_brutes.revenu_median > 0 && (
                      <div className="bg-paper rounded-xl p-4">
                        <div className="font-mono text-xl font-bold text-ink">
                          {Math.round(data.score.donnees_brutes.revenu_median).toLocaleString('fr-FR')} €
                        </div>
                        <div className="text-xs text-ink-light mt-1">revenu médian / an</div>
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
                  </div>
                </div>
              )}
            </div>
          )}

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
              Scores basés sur les données BPE 2024, Filosofi 2021 et DVF 2024.{' '}
              <Link to="/methode" className="underline hover:text-ink">En savoir plus sur la méthode</Link>
            </p>
          </div>
        </main>
      )}
    </div>
  )
}
