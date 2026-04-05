import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import ScoreCard from './ScoreCard.jsx'
import { CATEGORY_META, IRIS_CATEGORIES_LOCAL, IRIS_CATEGORIES_COMMUNE } from '../constants.js'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { scoreToColor } from '../utils/scoreUtils.js'

const TYP_IRIS_LABEL = {
  H: 'Résidentiel', A: 'Activité', D: 'Diversifié', Z: 'Commune entière',
}

export default function IrisPanel({ codeIris, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const navigate = useNavigate()

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
        // IRIS de type Z = commune entière → basculer sur commune
        if (d.typ_iris === 'Z') {
          navigate(`/commune/${d.code_commune}`, { replace: true })
          return
        }
        setData(d)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [codeIris])

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
        <p className="text-lg font-display text-ink mb-2">Quartier introuvable</p>
        <p className="text-sm text-ink-light">{error}</p>
      </div>
    )
  }

  if (!data) return null

  const brutes = data.score?.donnees_brutes || {}
  const detail = brutes.equipements_detail || {}
  const poi = brutes.poi_detail || {}
  const has = (bpe, p) => (bpe && detail[bpe] > 0) || (p && poi[p] > 0)

  // ── Helpers ──
  function airLabel(moy) {
    if (moy == null || moy < 0) return null
    if (moy < 1.5) return 'Bon'
    if (moy < 2.5) return 'Moyen'
    if (moy < 3.5) return 'Dégradé'
    return 'Mauvais'
  }
  function airColor(moy) {
    if (moy == null) return 'text-ink-light'
    if (moy < 1.5) return 'text-score-A'
    if (moy < 2.5) return 'text-score-B'
    if (moy < 3.5) return 'text-score-C'
    return 'text-score-D'
  }

  function catStat(key) {
    if (key === 'sante') {
      if (brutes.apl_medecins > 0) return `${brutes.apl_medecins.toFixed(1)} consult./an`
      if (brutes.medecins_pour_10000 > 0) return `${brutes.medecins_pour_10000.toFixed(1)} méd./10k`
    }
    if (key === 'immobilier' && brutes.prix_m2_median > 0)
      return `${Math.round(brutes.prix_m2_median).toLocaleString('fr-FR')} €/m²`
    if (key === 'environnement') {
      if (brutes.qualite_air_moy != null && brutes.qualite_air_moy >= 0)
        return <span>Air : <span className={airColor(brutes.qualite_air_moy)}>{airLabel(brutes.qualite_air_moy)}</span></span>
    }
    return null
  }

  const hasCommune = IRIS_CATEGORIES_COMMUNE.some(k => data.score?.sous_scores?.[k] != null)

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

  const codeCommune = data.code_commune || codeIris?.slice(0, 5)

  // POI pills
  const commercesPills = [['Boulangerie', 'boulangerie', 'boulangerie'], ['Supermarché', 'supermarché', 'supermarché'], ['Boucherie', 'boucherie', 'boucherie']]
    .filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)
  const santePills = [['Médecin', 'médecin_généraliste', null], ['Pharmacie', 'pharmacie', 'pharmacie'], ['Hôpital', 'hôpital', 'hôpital']]
    .filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)
  const educPills = [['Maternelle', 'école_maternelle', 'école_maternelle'], ['Collège', 'collège', 'collège'], ['Lycée', 'lycée', 'lycée']]
    .filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)

  const prixVal = brutes.prix_m2_median > 0 ? `${Math.round(brutes.prix_m2_median).toLocaleString('fr-FR')} €` : null

  return (
    <div className="space-y-4">
      {/* ── Header ── */}
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-1.5 mb-1">
            <span className="text-[10px] font-mono bg-paper border border-border rounded px-1.5 py-0.5 text-ink-light">IRIS</span>
            {data.typ_iris && <span className="text-[10px] text-ink-light">{TYP_IRIS_LABEL[data.typ_iris]}</span>}
          </div>
          <h2 className="font-display text-xl sm:text-2xl text-ink leading-tight">{data.nom}</h2>
          <div className="flex flex-wrap items-center gap-1.5 text-xs text-ink-light mt-1">
            {data.population > 0 && <span>{data.population.toLocaleString('fr-FR')} hab.</span>}
            <button onClick={() => navigate(`/commune/${codeCommune}`)} className="underline hover:text-ink">
              {data.commune_nom || codeCommune}
            </button>
          </div>
        </div>
        {data.score && data.score.lettre && (
          <ScoreCard lettre={data.score.lettre} score={data.score.score_global} size="sm" />
        )}
      </div>

      {/* Rang dans la commune */}
      {data.rang_commune > 0 && data.nb_iris_commune > 0 && (
        <div className="flex items-center gap-3 bg-paper border border-border rounded-lg px-3 py-2">
          <div className="text-center flex-shrink-0">
            <div className="font-mono text-lg font-bold text-ink">#{data.rang_commune}</div>
            <div className="text-[10px] text-ink-light">sur {data.nb_iris_commune}</div>
          </div>
          <div className="flex-1">
            <div className="w-full h-1.5 bg-border rounded-full overflow-hidden">
              <div className="h-full bg-score-A rounded-full" style={{ width: `${Math.max(4, 100 - ((data.rang_commune - 1) / data.nb_iris_commune) * 100)}%` }} />
            </div>
          </div>
          <span className="text-[10px] text-ink-light flex-shrink-0">
            {data.rang_commune === 1 ? 'Meilleur' : data.rang_commune <= data.nb_iris_commune * 0.25 ? 'Top 25%' : data.rang_commune <= data.nb_iris_commune * 0.75 ? 'Moyenne' : 'En dessous'}
          </span>
        </div>
      )}

      {/* Commune context */}
      {data.commune_score && (
        <button onClick={() => navigate(`/commune/${codeCommune}`)} className="w-full flex items-center gap-3 bg-paper border border-border rounded-lg px-3 py-2 hover:border-ink/40 transition-all">
          <span className="flex-1 text-xs text-ink-light text-left">Commune : <strong className="text-ink">{data.commune_nom}</strong></span>
          <span className="font-mono text-xs text-ink-light">{Math.round(data.commune_score.score_global)}</span>
          <div className={`score-badge w-6 h-6 text-[10px] score-badge-${data.commune_score.lettre}`}>{data.commune_score.lettre}</div>
        </button>
      )}

      {/* ── Catégories barres ── */}
      {data.score && (
        <div className="bg-white rounded-xl border border-border p-4">
          <div className="space-y-2">
            {[...IRIS_CATEGORIES_LOCAL]
              .sort((a, b) => (data.score.sous_scores?.[b] ?? -1) - (data.score.sous_scores?.[a] ?? -1))
              .map(k => {
                const meta = CATEGORY_META[k]
                const val = data.score.sous_scores?.[k]
                if (val == null) return null
                const pct = Math.round(val)
                const color = scoreToColor(pct)
                  return (
                  <div key={k} className="flex items-center gap-2">
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

          {hasCommune && (
            <>
              <div className="flex items-center gap-2 my-3">
                <div className="flex-1 h-px bg-border" />
                <span className="text-[10px] text-ink-light">Données ville</span>
                <div className="flex-1 h-px bg-border" />
              </div>
              <div className="space-y-2">
                {[...IRIS_CATEGORIES_COMMUNE]
                  .sort((a, b) => (data.score.sous_scores?.[b] ?? -1) - (data.score.sous_scores?.[a] ?? -1))
                  .map(k => {
                    const meta = CATEGORY_META[k]
                    const val = data.score.sous_scores?.[k]
                    if (val == null) return null
                    const pct = Math.round(val)
                    const color = scoreToColor(pct)
                    return (
                      <div key={k} className="flex items-center gap-2">
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
            </>
          )}
        </div>
      )}

      {/* ── Stats chiffrées ── */}
      <div className="bg-white rounded-xl border border-border overflow-hidden">
        <div className="grid grid-cols-2 divide-x divide-border border-b border-border">
          <Stat label="prix / m²" value={prixVal} />
          {brutes.taux_criminalite != null && (
            <Stat label="délits / 1 000 hab" value={`${brutes.taux_criminalite.toFixed(1)}`} sub="ville" />
          )}
        </div>
        {(brutes.revenu_median > 0 || brutes.taux_pauvrete > 0) && (
          <div className="grid grid-cols-2 divide-x divide-border">
            {brutes.revenu_median > 0 && <Stat label="revenu médian" value={`${Math.round(brutes.revenu_median).toLocaleString('fr-FR')} €`} />}
            {brutes.taux_pauvrete > 0 && <Stat label="pauvreté" value={`${brutes.taux_pauvrete.toFixed(1)} %`} />}
          </div>
        )}
      </div>

      {/* ── Équipements pills ── */}
      {(commercesPills.length > 0 || santePills.length > 0 || educPills.length > 0) && (
        <div className="bg-white rounded-xl border border-border p-4 space-y-2">
          {commercesPills.length > 0 && <div className="flex flex-wrap gap-1">{commercesPills}</div>}
          {santePills.length > 0 && <div className="flex flex-wrap gap-1">{santePills}</div>}
          {educPills.length > 0 && <div className="flex flex-wrap gap-1">{educPills}</div>}
        </div>
      )}
    </div>
  )
}
