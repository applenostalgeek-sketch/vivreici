import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { getCommune, getClassement } from '../hooks/useSearch.js'
import ScoreCard from './ScoreCard.jsx'
import { CATEGORY_META } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { useProfile } from '../context/ProfileContext.jsx'
import { PROFILES, recalcScore } from '../utils/profiles.js'
import { scoreToColor } from '../utils/scoreUtils.js'

export default function CommunePanel({ codeInsee, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [voisines, setVoisines] = useState([])
  const navigate = useNavigate()
  const { profile } = useProfile()

  usePageMeta({
    title: data ? `${data.nom} — ${data.score?.lettre || '?'} · ${Math.round(data.score?.score_global || 0)}/100` : 'Commune',
    description: data ? `Score de qualité de vie de ${data.nom} (${data.departement}) : ${data.score?.lettre || '?'} (${Math.round(data.score?.score_global || 0)}/100).` : null,
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

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20 text-ink-light">
        <div className="w-5 h-5 border-2 border-border border-t-ink rounded-full animate-spin mr-2" />
        Chargement…
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-center py-12 px-4">
        <p className="text-lg font-display text-ink mb-2">Commune introuvable</p>
        <p className="text-sm text-ink-light">{error}</p>
      </div>
    )
  }

  if (!data) return null

  const brutes = data.score?.donnees_brutes || {}
  const poi = brutes.poi_detail || {}

  // ── Helpers ──────────────────────────────────────────────────
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

  function catStat(key) {
    if (key === 'securite' && brutes.taux_criminalite != null)
      return `${brutes.taux_criminalite.toFixed(1)} ‰`
    if (key === 'sante' && brutes.apl_medecins > 0)
      return `${brutes.apl_medecins.toFixed(1)} consult./an`
    if (key === 'immobilier' && brutes.prix_m2_median > 0) {
      const prix = Math.round(brutes.prix_m2_median).toLocaleString('fr-FR')
      const p22 = brutes.prix_m2_median_2022
      if (p22 > 0) {
        const pct = Math.round((brutes.prix_m2_median - p22) / p22 * 100)
        const arrow = pct > 2 ? '↑' : pct < -2 ? '↓' : '→'
        const cls = pct > 2 ? 'text-score-D' : pct < -2 ? 'text-score-B' : 'text-ink-muted'
        return <span>{prix} €/m² <span className={`font-mono ${cls}`}>{arrow}{pct > 0 ? '+' : ''}{pct}%</span></span>
      }
      return `${prix} €/m²`
    }
    if (key === 'transports' && brutes.nom_gare && !['2A','2B'].includes(data.departement))
      return brutes.distance_gare_km >= 2 ? `Gare ${brutes.distance_gare_km} km` : brutes.nom_gare
    if (key === 'demographie' && brutes.evolution_population_5ans != null && brutes.evolution_population_5ans !== 0)
      return `${brutes.evolution_population_5ans > 0 ? '+' : ''}${brutes.evolution_population_5ans.toFixed(1)}% / 5 ans`
    if (key === 'education' && brutes.min_dist_college_km > 0) {
      const dist = brutes.min_dist_college_km
      return dist < 1 ? 'Collège < 1 km' : `Collège ${dist.toFixed(1)} km`
    }
    if (key === 'environnement') {
      const parts = []
      if (brutes.taux_espaces_nat != null && brutes.taux_espaces_nat >= 0)
        parts.push(`${Math.round(brutes.taux_espaces_nat)}% nat.`)
      if (brutes.qualite_air_moy != null && brutes.qualite_air_moy >= 0)
        return <span>{parts.length > 0 && <>{parts[0]} · </>}Air : <span className={airColor(brutes.qualite_air_moy)}>{airLabel(brutes.qualite_air_moy)}</span></span>
      return parts.length > 0 ? parts.join(' · ') : null
    }
    if (key === 'risques') return brutes.risques_detail ? null : 'Aucun PPR'
    return null
  }

  const sortedCats = data.score
    ? Object.entries(CATEGORY_META)
        .map(([key, meta]) => ({ key, meta, val: data.score.sous_scores[key] }))
        .filter(({ val }) => val != null)
        .sort((a, b) => b.val - a.val)
    : []

  const profileResult = data.score ? recalcScore(data.score.sous_scores, profile) : null
  const displayLettre = profileResult?.lettre ?? data.score?.lettre
  const displayScore = profileResult?.score_global ?? data.score?.score_global

  // ── Pill helpers ──────────────────────────────────────────────
  const Pill = ({ label, accent }) => (
    <span className={`text-[11px] px-2 py-0.5 rounded-full border ${accent || 'bg-paper text-ink-light border-border'}`}>
      {label}
    </span>
  )
  const Stat = ({ label, value, sub }) => value != null && (
    <div className="px-3 py-2">
      <div className="font-mono text-sm font-semibold text-ink">{value}</div>
      <div className="text-[10px] text-ink-muted mt-0.5">{label}</div>
      {sub && <div className="text-[10px] text-ink-light mt-0.5">{sub}</div>}
    </div>
  )

  // POI pills
  const makePills = (items) => items.filter(([, v]) => v > 0).map(([l]) => <Pill key={l} label={l} />)
  const commercesPills = makePills([['Boulangerie', poi.boulangerie], ['Supermarché', poi.supermarché], ['Boucherie', poi.boucherie]])
  const santePills = makePills([['Pharmacie', poi.pharmacie], ['Hôpital', poi.hôpital], ['Clinique', poi.clinique]])
  const educPills = makePills([['Maternelle', poi.école_maternelle], ['Primaire', poi.école_primaire], ['Collège', poi.collège], ['Lycée', poi.lycée]])
  const sportsPills = makePills([['Piscine', poi.piscine], ['Gymnase', poi.gymnase], ['Stade', poi.stade]])
  const culturePills = makePills([['Cinéma', poi.cinéma], ['Bibliothèque', poi.bibliothèque], ['Théâtre', poi.théâtre], ['Musée', poi.musée]])

  // Transport
  const TYPE_META = {
    1: { label: 'Métro', icon: '🚇' }, 0: { label: 'Tramway', icon: '🚃' },
    11: { label: 'Tramway', icon: '🚃' }, 3: { label: 'Bus', icon: '🚌' },
    7: { label: 'Bus', icon: '🚌' }, 700: { label: 'Bus', icon: '🚌' },
  }
  const lignes = brutes.transport_detail?.lignes || []
  const tPills = []
  const seenTC = new Set()
  const gareLabel = brutes.nom_gare && !['2A','2B'].includes(data.departement)
    ? (brutes.distance_gare_km >= 2 ? `${brutes.nom_gare} — ${brutes.distance_gare_km} km` : brutes.nom_gare)
    : null
  if (gareLabel) tPills.push(<Pill key="gare" label={`🚉 ${gareLabel}`} accent="bg-blue-50 text-blue-800 border-blue-200" />)
  for (const l of lignes) {
    if (l.type_code === 2) continue
    const m = TYPE_META[l.type_code]
    if (m && !seenTC.has(m.label)) { seenTC.add(m.label); tPills.push(<Pill key={m.label} label={`${m.icon} ${m.label}`} accent="bg-blue-50 text-blue-800 border-blue-200" />) }
  }

  // Risques
  const RISK_META = {
    inondation: { label: 'Inondation', icon: '🌊' }, seisme: { label: 'Séisme', icon: '🏔️' },
    mvt_terrain: { label: 'Mvt terrain', icon: '⛰️' }, foret: { label: 'Feu forêt', icon: '🔥' },
    avalanche: { label: 'Avalanche', icon: '❄️' },
  }
  const riskTags = (brutes.risques_detail || '').split(',').filter(Boolean).filter(r => RISK_META[r]).map(r => RISK_META[r])
  const riskPills = riskTags.length > 0
    ? riskTags.map(r => <Pill key={r.label} label={`${r.icon} ${r.label}`} accent="bg-amber-50 text-amber-800 border-amber-200" />)
    : [<Pill key="none" label="✓ Aucun PPR" accent="bg-green-50 text-green-800 border-green-200" />]

  const prixVal = brutes.prix_m2_median > 0 ? `${Math.round(brutes.prix_m2_median).toLocaleString('fr-FR')} €` : null
  let prixSub = null
  if (brutes.prix_m2_median > 0 && brutes.prix_m2_median_2022 > 0) {
    const pct = Math.round((brutes.prix_m2_median - brutes.prix_m2_median_2022) / brutes.prix_m2_median_2022 * 100)
    prixSub = `${pct > 0 ? '+' : ''}${pct}% depuis 2022`
  }
  if (brutes.prix_m2_estime) prixSub = [prixSub, 'estimation'].filter(Boolean).join(' · ')

  return (
    <div className="space-y-4">
      {/* ── Header ── */}
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="font-display text-xl sm:text-2xl text-ink leading-tight">{data.nom}</h2>
          <div className="flex flex-wrap items-center gap-1.5 text-xs text-ink-light mt-1">
            {data.departement && <span>{data.departement}</span>}
            {data.population > 0 && <><span>·</span><span>{data.population.toLocaleString('fr-FR')} hab.</span></>}
            {data.codes_postaux?.length > 0 && <span className="font-mono">{data.codes_postaux[0]}</span>}
          </div>
          {data.rang_departement && data.nb_communes_departement && (
            <p className="text-xs text-ink-light mt-1">
              {data.rang_departement === 1
                ? <strong className="text-ink">1ère du département</strong>
                : <><strong className="text-ink">{data.rang_departement}ème</strong> / {data.nb_communes_departement}</>}
            </p>
          )}
        </div>
        {data.score && displayLettre && (
          <ScoreCard lettre={displayLettre} score={displayScore} size="sm" />
        )}
      </div>

      {/* Profil indicator */}
      {profileResult && (
        <div className="flex items-center gap-1.5">
          <span className="flex items-center gap-1 px-2 py-0.5 rounded-full bg-ink text-paper text-[10px] font-medium">
            {PROFILES[profile].emoji} {PROFILES[profile].label}
          </span>
          <span className="text-[10px] text-ink-light">
            national : {data.score.lettre} · {Math.round(data.score.score_global)}
          </span>
        </div>
      )}

      {/* ── Catégories barres ── */}
      {sortedCats.length > 0 && (
        <div className="bg-white rounded-xl border border-border p-4">
          <div className="space-y-2">
            {sortedCats.map(({ key, meta, val }) => {
              const pct = Math.round(val)
              const color = scoreToColor(pct)
              return (
                <div key={key} className="flex items-center gap-2">
                  <span className="text-xs w-4 flex-shrink-0 text-center">{meta.icon}</span>
                  <span className="text-[10px] font-semibold text-ink-light w-20 flex-shrink-0 truncate">{meta.label}</span>
                  <div className="flex-1 h-5 bg-border rounded-md overflow-hidden relative">
                    <div className="h-full rounded-md" style={{ width: `${Math.max(pct, 2)}%`, backgroundColor: color, backgroundImage: 'linear-gradient(90deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.18) 100%)' }} />
                    <span className="absolute inset-y-0 flex items-center font-display text-[10px] font-bold leading-none text-white" style={{ left: pct >= 12 ? `${Math.max(pct, 2) - 1}%` : `${Math.max(pct, 2) + 1}%`, transform: pct >= 12 ? 'translateX(-100%)' : 'none' }}>{pct}</span>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* ── Stats chiffrées ── */}
      <div className="bg-white rounded-xl border border-border overflow-hidden">
        <div className="grid grid-cols-2 divide-x divide-border border-b border-border">
          <Stat label="prix / m²" value={prixVal} sub={prixSub} />
          {brutes.taux_criminalite != null && (
            <Stat label="délits / 1 000 hab" value={`${brutes.taux_criminalite.toFixed(1)}`} />
          )}
        </div>
        <div className="grid grid-cols-2 divide-x divide-border border-b border-border">
          {brutes.apl_medecins > 0 && (
            <Stat label="consult. méd." value={`${brutes.apl_medecins.toFixed(1)} / an`} />
          )}
          {brutes.taux_espaces_nat != null && brutes.taux_espaces_nat >= 0 && (
            <Stat label="espaces naturels" value={`${Math.round(brutes.taux_espaces_nat)} %`} />
          )}
        </div>
        {(brutes.revenu_median > 0 || brutes.taux_pauvrete > 0) && (
          <div className="grid grid-cols-2 divide-x divide-border">
            {brutes.revenu_median > 0 && (
              <Stat label="revenu médian / an" value={`${Math.round(brutes.revenu_median).toLocaleString('fr-FR')} €`} />
            )}
            {brutes.taux_pauvrete > 0 && (
              <Stat label="taux pauvreté" value={`${brutes.taux_pauvrete.toFixed(1)} %`} />
            )}
          </div>
        )}
      </div>

      {/* ── Équipements pills ── */}
      {(commercesPills.length > 0 || santePills.length > 0 || educPills.length > 0 || tPills.length > 0 || sportsPills.length > 0 || culturePills.length > 0) && (
        <div className="bg-white rounded-xl border border-border p-4 space-y-3">
          {commercesPills.length > 0 && <div className="flex flex-wrap gap-1">{commercesPills}</div>}
          {santePills.length > 0 && <div className="flex flex-wrap gap-1">{santePills}</div>}
          {educPills.length > 0 && <div className="flex flex-wrap gap-1">{educPills}</div>}
          {tPills.length > 0 && <div className="flex flex-wrap gap-1">{tPills}</div>}
          {sportsPills.length > 0 && <div className="flex flex-wrap gap-1">{sportsPills}</div>}
          {culturePills.length > 0 && <div className="flex flex-wrap gap-1">{culturePills}</div>}
          <div className="flex flex-wrap gap-1">{riskPills}</div>
        </div>
      )}

      {/* ── Quartiers IRIS ── */}
      {data.iris && data.iris.length > 0 && (() => {
        const complete = data.iris.filter(z => !z.donnees_partielles)
        return complete.length > 0 && (
          <div>
            <h3 className="font-display text-sm text-ink mb-2">Quartiers</h3>
            <div className="space-y-1">
              {complete.slice(0, 8).map((iris, i) => (
                <button
                  key={iris.code_iris}
                  onClick={() => navigate(`/iris/${iris.code_iris}`)}
                  className="w-full flex items-center gap-2 bg-white border border-border rounded-lg px-3 py-2 hover:border-ink/40 transition-all text-left group"
                >
                  <span className="font-mono text-[10px] text-ink-light w-4 flex-shrink-0">{i + 1}</span>
                  <span className="flex-1 text-xs text-ink group-hover:underline truncate">{iris.nom}</span>
                  {iris.lettre && (
                    <div className={`score-badge w-6 h-6 text-[10px] score-badge-${iris.lettre}`}>{iris.lettre}</div>
                  )}
                </button>
              ))}
            </div>
            {complete.length > 8 && (
              <button
                onClick={() => navigate(`/commune/${data.code_insee}/quartiers`)}
                className="mt-2 text-xs text-ink-light underline hover:text-ink"
              >
                Voir les {complete.length} quartiers →
              </button>
            )}
          </div>
        )
      })()}

      {/* ── Top département ── */}
      {voisines.length > 0 && (
        <div>
          <h3 className="font-display text-sm text-ink mb-2">Top — Dept. {data.departement}</h3>
          <div className="space-y-1">
            {voisines.slice(0, 5).map(c => (
              <button
                key={c.code_insee}
                onClick={() => navigate(`/commune/${c.code_insee}`)}
                className="w-full flex items-center gap-2 bg-white border border-border rounded-lg px-3 py-2 hover:border-ink/40 transition-all text-left group"
              >
                <span className="flex-1 text-xs text-ink group-hover:underline truncate">{c.nom}</span>
                {c.score && (
                  <div className="flex items-center gap-1.5 flex-shrink-0">
                    <span className="font-mono text-[10px] text-ink-light">{Math.round(c.score.score_global)}</span>
                    <div className={`score-badge w-6 h-6 text-[10px] score-badge-${c.score.lettre}`}>{c.score.lettre}</div>
                  </div>
                )}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
