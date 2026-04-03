import { useParams, Link, useNavigate, useSearchParams } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { getCommune, getClassement } from '../hooks/useSearch.js'
import ScoreCard from '../components/ScoreCard.jsx'
import Nav from '../components/Nav.jsx'
import ScoreBar from '../components/ScoreBar.jsx'
import MapView from '../components/MapView.jsx'
import { CATEGORY_META } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { useProfile } from '../context/ProfileContext.jsx'
import { PROFILES, recalcScore } from '../utils/profiles.js'
import { scoreToColor, scoreToTextClass } from '../utils/scoreUtils.js'

export default function Commune() {
  const { codeInsee } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()
  const tab = searchParams.get('tab') || 'detail'
  const markerLat = searchParams.get('lat') ? parseFloat(searchParams.get('lat')) : null
  const markerLng = searchParams.get('lng') ? parseFloat(searchParams.get('lng')) : null

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [voisines, setVoisines] = useState([])
  const navigate = useNavigate()
  const { profile } = useProfile()

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
        <>
        <main className="max-w-4xl mx-auto px-6 py-12">
          {/* Tabs CARTE / DETAIL */}
          <div className="flex items-center gap-1 mb-8 bg-paper border border-border rounded-xl p-1 w-fit">
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
          </div>

          {/* ── TAB CARTE ──────────────────────────────────────────────── */}
          {tab === 'carte' && data.latitude && data.longitude && (
            <div>
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
              <h1 className="font-display text-4xl md:text-5xl text-ink mb-2">{data.nom}</h1>
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

            {data.score ? (() => {
              const profileResult = recalcScore(data.score.sous_scores, profile)
              const displayLettre = profileResult?.lettre ?? data.score.lettre
              const displayScore = profileResult?.score_global ?? data.score.score_global
              return (
              <div className="flex-shrink-0 flex flex-col items-center gap-2">
                <ScoreCard lettre={displayLettre} score={displayScore} size="lg" />
                {profileResult && (
                  <div className="flex items-center gap-1.5 mt-1">
                    <span className="flex items-center gap-1 px-2.5 py-1 rounded-full bg-ink text-paper text-xs font-medium">
                      {PROFILES[profile].emoji} {PROFILES[profile].label}
                    </span>
                    <span className="text-xs text-ink-light">
                      national : {data.score.lettre} · {Math.round(data.score.score_global)}
                    </span>
                  </div>
                )}
              </div>
              )
            })() : (
              <div className="bg-paper border border-border rounded-2xl px-6 py-4 text-center">
                <p className="text-ink-light text-sm">Score en cours de calcul</p>
              </div>
            )}
          </div>

          {data.score && (() => {
            const brutes = data.score.donnees_brutes || {}

            // ── Stat inline par catégorie ────────────────────────────────────
            function airLabel(moy) {
              if (moy == null || moy < 0) return null
              if (moy < 1.5) return 'Bon'
              if (moy < 2.5) return 'Moyen'
              if (moy < 3.5) return 'Dégradé'
              if (moy < 4.5) return 'Mauvais'
              if (moy < 5.5) return 'Très mauvais'
              return 'Extrêmement mauvais'
            }
            function airColor(moy) {
              if (moy == null) return 'text-ink-light'
              if (moy < 1.5) return 'text-score-A'
              if (moy < 2.5) return 'text-score-B'
              if (moy < 3.5) return 'text-score-C'
              return 'text-score-D'
            }

            function catStat(key, val) {
              if (key === 'securite' && brutes.taux_criminalite != null)
                return `${brutes.taux_criminalite.toFixed(1)} délits / 1 000 hab`
              if (key === 'sante' && brutes.apl_medecins > 0)
                return `${brutes.apl_medecins.toFixed(1)} consult./an/hab`
              if (key === 'immobilier' && brutes.prix_m2_median > 0) {
                const prix = Math.round(brutes.prix_m2_median).toLocaleString('fr-FR')
                const p22 = brutes.prix_m2_median_2022
                if (p22 > 0) {
                  const pct = Math.round((brutes.prix_m2_median - p22) / p22 * 100)
                  const arrow = pct > 2 ? '↑' : pct < -2 ? '↓' : '→'
                  const cls = pct > 2 ? 'text-score-D' : pct < -2 ? 'text-score-B' : 'text-ink-muted'
                  return <span>{prix} €/m² <span className={`font-mono ${cls}`}>{arrow} {pct > 0 ? '+' : ''}{pct}%</span></span>
                }
                return `${prix} €/m²`
              }
              if (key === 'transports' && brutes.nom_gare && !['2A','2B'].includes(data.departement))
                return brutes.distance_gare_km >= 2 ? `${brutes.nom_gare} — ${brutes.distance_gare_km} km` : brutes.nom_gare
              if (key === 'demographie' && brutes.evolution_population_5ans != null && brutes.evolution_population_5ans !== 0) {
                const evo = brutes.evolution_population_5ans
                return `${evo > 0 ? '+' : ''}${evo.toFixed(1)} % sur 5 ans`
              }
              if (key === 'education' && brutes.min_dist_college_km > 0) {
                const dist = brutes.min_dist_college_km
                return dist < 1 ? 'Collège < 1 km' : `Collège le + proche : ${dist.toFixed(1)} km`
              }
              if (key === 'environnement') {
                const parts = []
                if (brutes.taux_espaces_nat != null && brutes.taux_espaces_nat >= 0)
                  parts.push(`${Math.round(brutes.taux_espaces_nat)}% espaces nat.`)
                if (brutes.qualite_air_moy != null && brutes.qualite_air_moy >= 0)
                  return (
                    <span>
                      {parts.length > 0 && <>{parts[0]} · </>}
                      Air : <span className={airColor(brutes.qualite_air_moy)}>{airLabel(brutes.qualite_air_moy)}</span>
                    </span>
                  )
                return parts.length > 0 ? parts.join(' · ') : null
              }
              if (key === 'risques')
                return brutes.risques_detail ? null : 'Aucun PPR'
              return null
            }

            // Tri par score décroissant
            const sortedCats = Object.entries(CATEGORY_META)
              .map(([key, meta]) => ({ key, meta, val: data.score.sous_scores[key] }))
              .filter(({ val }) => val != null)
              .sort((a, b) => b.val - a.val)

            // ── Transport tags ────────────────────────────────────────────────
            const TYPE_META = {
              1: { label: 'Métro', icon: '🚇' }, 0: { label: 'Tramway', icon: '🚃' },
              11: { label: 'Tramway', icon: '🚃' }, 4: { label: 'Ferry', icon: '⛴️' },
              3: { label: 'Bus', icon: '🚌' }, 7: { label: 'Bus', icon: '🚌' },
              700: { label: 'Bus', icon: '🚌' }, 702: { label: 'Bus', icon: '🚌' },
            }
            const lignes = brutes.transport_detail?.lignes || []
            const transportTags = []
            const seenTC = new Set()
            const gareLabel = brutes.nom_gare && !['2A','2B'].includes(data.departement)
              ? (brutes.distance_gare_km >= 2 ? `${brutes.nom_gare} — ${brutes.distance_gare_km} km` : brutes.nom_gare)
              : null
            for (const l of lignes) {
              if (l.type_code === 2) continue
              const m = TYPE_META[l.type_code]
              if (m && !seenTC.has(m.label)) { seenTC.add(m.label); transportTags.push(m) }
            }

            // ── POI groupes ───────────────────────────────────────────────────
            const poi = brutes.poi_detail || {}
            const POI_GROUPES = [
              { label: 'Commerces', keys: [
                { key: 'boulangerie', label: 'Boulangerie' }, { key: 'boucherie', label: 'Boucherie' },
                { key: 'supermarché', label: 'Supermarché' },
              ]},
              { label: 'Santé', keys: [
                { key: 'pharmacie', label: 'Pharmacie' }, { key: 'cabinet_médical', label: 'Cabinet médical' },
                { key: 'hôpital', label: 'Hôpital' }, { key: 'clinique', label: 'Clinique' },
                { key: 'labo_analyse', label: 'Laboratoire' },
              ]},
              { label: 'Éducation', keys: [
                { key: 'école_maternelle', label: 'Maternelle' }, { key: 'école_primaire', label: 'Primaire' },
                { key: 'collège', label: 'Collège' }, { key: 'lycée', label: 'Lycée' },
                { key: 'lycée_professionnel', label: 'Lycée pro' },
              ]},
              { label: 'Sports', keys: [
                { key: 'piscine', label: 'Piscine' }, { key: 'gymnase', label: 'Gymnase' }, { key: 'stade', label: 'Stade' },
                { key: 'terrain_football', label: 'Terrain foot' }, { key: 'salle_sport', label: 'Salle de sport' },
              ]},
              { label: 'Culture', keys: [
                { key: 'cinéma', label: 'Cinéma' }, { key: 'bibliothèque', label: 'Bibliothèque' },
                { key: 'théâtre', label: 'Théâtre' }, { key: 'musée', label: 'Musée' },
              ]},
            ]
            const poiActifs = POI_GROUPES
              .map(g => ({ ...g, present: g.keys.filter(({ key }) => (poi[key] || 0) > 0).map(({ label }) => label) }))
              .filter(g => g.present.length > 0)

            // ── Risques tags ──────────────────────────────────────────────────
            const RISK_META = {
              inondation:  { label: 'Inondation', icon: '🌊' }, seisme: { label: 'Séisme', icon: '🏔️' },
              mvt_terrain: { label: 'Mouvement de terrain', icon: '⛰️' }, foret: { label: 'Feu de forêt', icon: '🔥' },
              avalanche:   { label: 'Avalanche', icon: '❄️' },
            }
            const riskTags = (brutes.risques_detail || '').split(',').filter(Boolean)
              .filter(r => RISK_META[r]).map(r => RISK_META[r])

            const hasSurPlace = gareLabel || transportTags.length || poiActifs.length

            return (
              <div className="space-y-4">

                {/* ── Catégories ── */}
                <div className="bg-white rounded-2xl border border-border p-6">
                  <div className="space-y-2.5">
                    {sortedCats.map(({ key, meta, val }) => {
                      const pct = Math.round(val)
                      const color = scoreToColor(pct)
                      const stat = catStat(key, val)
                      return (
                        <div key={key} className="flex items-center gap-3">
                          <span className="text-sm w-5 flex-shrink-0 text-center">{meta.icon}</span>
                          <span className="text-xs font-semibold text-ink-light w-24 flex-shrink-0">{meta.label}</span>
                          <div className="flex-1 h-7 bg-border rounded-lg overflow-hidden">
                            <div className="h-full rounded-lg flex items-center justify-end pr-2" style={{ width: `${pct}%`, backgroundColor: color, backgroundImage: 'linear-gradient(90deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.18) 100%)' }}>
                              <span className="font-display text-white text-xs font-bold leading-none">{pct}</span>
                            </div>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                  {data.score.nb_categories_scorees < 6 && (
                    <p className="mt-4 text-xs text-ink-light border-t border-border pt-4">
                      Score calculé sur <strong className="text-ink">{data.score.nb_categories_scorees}</strong> catégorie(s) — données manquantes à venir.
                    </p>
                  )}
                </div>

                {/* ── Détails ── */}
                {(() => {
                  // Pill component (présence)
                  const Pill = ({ label, accent }) => (
                    <span className={`text-xs px-2.5 py-1 rounded-full border ${accent || 'bg-paper text-ink-light border-border'}`}>
                      {label}
                    </span>
                  )
                  // Stat component (chiffre)
                  const Stat = ({ label, value, sub }) => value != null && (
                    <div className="px-4 py-3">
                      <div className="font-mono text-lg font-semibold text-ink">{value}</div>
                      <div className="text-xs text-ink-muted mt-0.5">{label}</div>
                      {sub && <div className="text-xs text-ink-light mt-0.5">{sub}</div>}
                    </div>
                  )
                  // Pill row (label + pills)
                  const PillRow = ({ label, items }) => items.length > 0 && (
                    <div className="flex items-start gap-3">
                      <span className="text-xs text-ink-muted w-20 flex-shrink-0 pt-0.5">{label}</span>
                      <div className="flex flex-wrap gap-1.5">{items}</div>
                    </div>
                  )

                  // Données présence depuis poi_detail
                  const commercesPills = [
                    ['Boulangerie', poi.boulangerie], ['Supermarché', poi.supermarché],
                    ['Boucherie', poi.boucherie],
                  ].filter(([,v]) => v > 0).map(([l]) => <Pill key={l} label={l} />)

                  const santePills = [
                    ['Pharmacie', poi.pharmacie], ['Cabinet médical', poi.cabinet_médical],
                    ['Hôpital', poi.hôpital], ['Clinique', poi.clinique],
                    ['Laboratoire', poi.labo_analyse],
                  ].filter(([,v]) => v > 0).map(([l]) => <Pill key={l} label={l} />)

                  const educPills = [
                    ['Maternelle', poi.école_maternelle], ['Primaire', poi.école_primaire],
                    ['Collège', poi.collège], ['Lycée', poi.lycée],
                    ['Lycée pro', poi.lycée_professionnel],
                  ].filter(([,v]) => v > 0).map(([l]) => <Pill key={l} label={l} />)

                  const sportsPills = [
                    ['Piscine', poi.piscine], ['Gymnase', poi.gymnase],
                    ['Stade', poi.stade], ['Terrain foot', poi.terrain_football],
                  ].filter(([,v]) => v > 0).map(([l]) => <Pill key={l} label={l} />)

                  const culturePills = [
                    ['Cinéma', poi.cinéma], ['Bibliothèque', poi.bibliothèque],
                    ['Théâtre', poi.théâtre], ['Musée', poi.musée],
                  ].filter(([,v]) => v > 0).map(([l]) => <Pill key={l} label={l} />)

                  // Transport pills (gare + TC)
                  const tPills = []
                  if (gareLabel) tPills.push(<Pill key="gare" label={`🚉 ${gareLabel}`} accent="bg-blue-50 text-blue-800 border-blue-200" />)
                  transportTags.forEach(m => tPills.push(<Pill key={m.label} label={`${m.icon} ${m.label}`} accent="bg-blue-50 text-blue-800 border-blue-200" />))

                  // Risques pills
                  const riskPills = riskTags.length > 0
                    ? riskTags.map(r => <Pill key={r.label} label={`${r.icon} ${r.label}`} accent="bg-amber-50 text-amber-800 border-amber-200" />)
                    : [<Pill key="none" label="✓ Aucun PPR" accent="bg-green-50 text-green-800 border-green-200" />]

                  // Stats chiffrées
                  const prixVal = brutes.prix_m2_median > 0 ? `${Math.round(brutes.prix_m2_median).toLocaleString('fr-FR')} €` : null
                  let prixSub = null
                  if (brutes.prix_m2_median > 0 && brutes.prix_m2_median_2022 > 0) {
                    const pct = Math.round((brutes.prix_m2_median - brutes.prix_m2_median_2022) / brutes.prix_m2_median_2022 * 100)
                    prixSub = `${pct > 0 ? '+' : ''}${pct}% depuis 2022`
                  }
                  if (brutes.prix_m2_estime) prixSub = [prixSub, 'estimation KNN'].filter(Boolean).join(' · ')

                  const hasPillContent = commercesPills.length || santePills.length || educPills.length || sportsPills.length || culturePills.length || tPills.length

                  return (
                    <div className="bg-white rounded-2xl border border-border overflow-hidden">
                      <div className="px-5 py-3 border-b border-border bg-paper">
                        <p className="text-xs font-semibold uppercase tracking-wider text-ink-muted">Détails</p>
                      </div>

                      {/* ── Stats chiffrées ── */}
                      <div className="grid grid-cols-2 sm:grid-cols-4 divide-x divide-border border-b border-border">
                        <Stat label="prix / m²" value={prixVal} sub={prixSub} />
                        {brutes.taux_criminalite != null && (
                          <Stat label="délits / 1 000 hab" value={`${brutes.taux_criminalite.toFixed(1)}`} />
                        )}
                        {brutes.apl_medecins > 0 && (
                          <Stat label="consult. méd. / an / hab" value={brutes.apl_medecins.toFixed(1)} />
                        )}
                        {brutes.evolution_population_5ans != null && brutes.evolution_population_5ans !== 0 && (
                          <Stat label="population sur 5 ans" value={`${brutes.evolution_population_5ans > 0 ? '+' : ''}${brutes.evolution_population_5ans.toFixed(1)} %`} />
                        )}
                      </div>
                      <div className="grid grid-cols-2 sm:grid-cols-4 divide-x divide-border border-b border-border">
                        {brutes.taux_espaces_nat != null && brutes.taux_espaces_nat >= 0 && (
                          <Stat label="espaces naturels" value={`${Math.round(brutes.taux_espaces_nat)} %`} />
                        )}
                        {brutes.qualite_air_moy != null && brutes.qualite_air_moy >= 0 && (
                          <Stat label="qualité de l'air" value={<span className={airColor(brutes.qualite_air_moy)}>{airLabel(brutes.qualite_air_moy)}</span>} sub={`ATMO ${brutes.qualite_air_moy.toFixed(1)} / 6`} />
                        )}
                        {brutes.revenu_median > 0 && (
                          <Stat label="revenu médian / an" value={`${Math.round(brutes.revenu_median).toLocaleString('fr-FR')} €`} />
                        )}
                        {brutes.taux_pauvrete > 0 && (
                          <Stat label="taux de pauvreté" value={`${brutes.taux_pauvrete.toFixed(1)} %`} />
                        )}
                      </div>

                      {/* ── Présence (pills par section) ── */}
                      {(commercesPills.length > 0 || santePills.length > 0 || educPills.length > 0) && (
                        <div className="px-5 pt-4 pb-3 border-b border-border">
                          <p className="text-xs font-semibold uppercase tracking-wider text-ink-muted mb-3">Vie quotidienne</p>
                          <div className="space-y-3">
                            <PillRow label="Commerces" items={commercesPills} />
                            <PillRow label="Santé" items={santePills} />
                            <PillRow label="Éducation" items={educPills} />
                          </div>
                        </div>
                      )}

                      {tPills.length > 0 && (
                        <div className="px-5 pt-4 pb-3 border-b border-border">
                          <p className="text-xs font-semibold uppercase tracking-wider text-ink-muted mb-3">Logement & accès</p>
                          <div className="space-y-3">
                            <PillRow label="Transports" items={tPills} />
                          </div>
                        </div>
                      )}

                      <div className="px-5 pt-4 pb-3 border-b border-border">
                        <p className="text-xs font-semibold uppercase tracking-wider text-ink-muted mb-3">Cadre de vie</p>
                        <div className="space-y-3">
                          <PillRow label="Risques" items={riskPills} />
                        </div>
                      </div>

                      {(sportsPills.length > 0 || culturePills.length > 0) && (
                        <div className="px-5 pt-4 pb-3">
                          <p className="text-xs font-semibold uppercase tracking-wider text-ink-muted mb-3">Sports & culture</p>
                          <div className="space-y-3">
                            <PillRow label="Sports" items={sportsPills} />
                            <PillRow label="Culture" items={culturePills} />
                          </div>
                        </div>
                      )}
                    </div>
                  )
                })()}

                {/* Ancien bloc "Sur place" supprimé — intégré dans Détails ci-dessus */}

              </div>
            )
          })()}

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

          </>)} {/* fin tab detail */}
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
