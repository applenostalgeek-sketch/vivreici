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

  const hasCommune = IRIS_CATEGORIES_COMMUNE.some(k => data.score?.sous_scores?.[k] != null)

  // ── Pill / Stat / PillRow ──
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
  const PillRow = ({ label, items }) => items.length > 0 && (
    <div className="flex items-start gap-2">
      <span className="text-[10px] text-ink-muted w-16 flex-shrink-0 pt-0.5">{label}</span>
      <div className="flex flex-wrap gap-1">{items}</div>
    </div>
  )

  const codeCommune = data.code_commune || codeIris?.slice(0, 5)

  // ── POI + BPE pills ──
  const commercesPills = [
    ['Boulangerie', 'boulangerie', 'boulangerie'],
    ['Supermarché', 'supermarché', 'supermarché'],
    ['Boucherie', 'boucherie', 'boucherie'],
  ].filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)

  const santePills = [
    ['Médecin', 'médecin_généraliste', null],
    ['Pharmacie', 'pharmacie', 'pharmacie'],
    ['Hôpital', 'hôpital', 'hôpital'],
    ['Clinique', null, 'clinique'],
    ['Laboratoire', null, 'labo_analyse'],
  ].filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)

  const educPills = [
    ['Maternelle', 'école_maternelle', 'école_maternelle'],
    ['Primaire', 'école_élémentaire', 'école_primaire'],
    ['Collège', 'collège', 'collège'],
    ['Lycée', 'lycée', 'lycée'],
    ['Lycée pro', null, 'lycée_professionnel'],
  ].filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)

  const sportsPills = [
    ['Piscine', null, 'piscine'],
    ['Gymnase', 'gymnase', 'gymnase'],
    ['Stade', null, 'stade'],
    ['Terrain foot', 'terrain_football', null],
    ['Salle de sport', 'salle_sport', null],
  ].filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)

  const culturePills = [
    ['Cinéma', 'cinéma', 'cinéma'],
    ['Bibliothèque', 'bibliothèque', 'bibliothèque'],
    ['Théâtre', 'théâtre', 'théâtre'],
    ['Musée', null, 'musée'],
  ].filter(([, bpe, p]) => has(bpe, p)).map(([l]) => <Pill key={l} label={l} />)

  // ── Transport pills (données commune) ──
  const TYPE_META = {
    1: { label: 'Métro', icon: '🚇' }, 0: { label: 'Tramway', icon: '🚃' },
    11: { label: 'Tramway', icon: '🚃' }, 4: { label: 'Ferry', icon: '⛴️' },
    3: { label: 'Bus', icon: '🚌' }, 7: { label: 'Bus', icon: '🚌' },
    700: { label: 'Bus', icon: '🚌' }, 702: { label: 'Bus', icon: '🚌' },
  }
  const lignes = brutes.transport_detail?.lignes || []
  const tPills = []
  const seenTC = new Set()
  const gareLabel = brutes.nom_gare
    ? (brutes.distance_gare_km >= 2 ? `${brutes.nom_gare} — ${brutes.distance_gare_km} km` : brutes.nom_gare)
    : null
  if (gareLabel) tPills.push(<Pill key="gare" label={`🚉 ${gareLabel}`} accent="bg-blue-50 text-blue-800 border-blue-200" />)
  for (const l of lignes) {
    if (l.type_code === 2) continue
    const m = TYPE_META[l.type_code]
    if (m && !seenTC.has(m.label)) { seenTC.add(m.label); tPills.push(<Pill key={m.label} label={`${m.icon} ${m.label}`} accent="bg-blue-50 text-blue-800 border-blue-200" />) }
  }

  // ── Risques pills (données commune) ──
  const RISK_META = {
    inondation: { label: 'Inondation', icon: '🌊' }, seisme: { label: 'Séisme', icon: '🏔️' },
    mvt_terrain: { label: 'Mouvement de terrain', icon: '⛰️' }, foret: { label: 'Feu de forêt', icon: '🔥' },
    avalanche: { label: 'Avalanche', icon: '❄️' },
  }
  const riskTags = (brutes.risques_detail || '').split(',').filter(Boolean).filter(r => RISK_META[r]).map(r => RISK_META[r])
  const riskPills = riskTags.length > 0
    ? riskTags.map(r => <Pill key={r.label} label={`${r.icon} ${r.label}`} accent="bg-amber-50 text-amber-800 border-amber-200" />)
    : [<Pill key="none" label="✓ Aucun PPR" accent="bg-green-50 text-green-800 border-green-200" />]

  // ── Stats values ──
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
        {data.score && data.score.lettre ? (
          <ScoreCard lettre={data.score.lettre} score={data.score.score_global} size="sm" />
        ) : data.score ? (
          <div className="bg-paper border border-border rounded-xl px-4 py-2 text-center flex-shrink-0">
            <div className="font-mono text-lg font-bold text-ink-light">?</div>
            <p className="text-[10px] text-ink-light">{Math.round(data.score.score_global)} pts</p>
          </div>
        ) : null}
      </div>

      {/* Warning données partielles */}
      {data.score && data.score.nb_categories_scorees < 3 && (
        <div className="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2">
          <span className="text-amber-500 text-sm flex-shrink-0">⚠</span>
          <div>
            <div className="text-xs font-medium text-ink">Données partielles</div>
            <div className="text-[10px] text-ink-light mt-0.5">
              Score basé sur {data.score.nb_categories_scorees} catégorie(s) — insuffisant pour une lettre fiable.
            </div>
          </div>
        </div>
      )}

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

          {data.score.nb_categories_scorees < 6 && (
            <p className="mt-3 text-[10px] text-ink-light border-t border-border pt-3">
              Score calculé sur <strong className="text-ink">{data.score.nb_categories_scorees}</strong> catégorie(s) — même méthodologie que les communes.
            </p>
          )}
        </div>
      )}

      {/* ── Stats chiffrées ── */}
      <div className="bg-white rounded-xl border border-border overflow-hidden">
        <div className="px-4 py-2 border-b border-border bg-paper">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-ink-muted">Détails</p>
        </div>
        <div className="grid grid-cols-2 divide-x divide-border border-b border-border">
          <Stat label="prix / m²" value={prixVal} />
          {brutes.taux_criminalite != null && (
            <Stat label="délits / 1 000 hab" value={`${brutes.taux_criminalite.toFixed(1)}`} sub="données ville" />
          )}
        </div>
        <div className="grid grid-cols-2 divide-x divide-border border-b border-border">
          {brutes.apl_medecins > 0 && (
            <Stat label="consult. méd. / an / hab" value={brutes.apl_medecins.toFixed(1)} sub="données ville" />
          )}
          {brutes.taux_espaces_nat != null && brutes.taux_espaces_nat >= 0 && (
            <Stat label="espaces naturels" value={`${Math.round(brutes.taux_espaces_nat)} %`} sub="données ville" />
          )}
        </div>
        <div className="grid grid-cols-2 divide-x divide-border border-b border-border">
          {brutes.qualite_air_moy != null && brutes.qualite_air_moy >= 0 && (
            <Stat label="qualité de l'air" value={<span className={airColor(brutes.qualite_air_moy)}>{airLabel(brutes.qualite_air_moy)}</span>} sub={`ATMO ${brutes.qualite_air_moy.toFixed(1)} / 6`} />
          )}
          {brutes.revenu_median > 0 && (
            <Stat label="revenu médian / an" value={`${Math.round(brutes.revenu_median).toLocaleString('fr-FR')} €`} />
          )}
        </div>
        <div className="grid grid-cols-2 divide-x divide-border border-b border-border">
          {brutes.taux_pauvrete > 0 && (
            <Stat label="taux de pauvreté" value={`${brutes.taux_pauvrete.toFixed(1)} %`} />
          )}
          {brutes.prix_m2_median > 0 && brutes.revenu_median > 0 && (
            <Stat label="pour acheter 80 m²" value={`${(brutes.prix_m2_median * 80 / brutes.revenu_median).toFixed(1)} ans`} />
          )}
        </div>

        {/* ── Vie quotidienne ── */}
        {(commercesPills.length > 0 || santePills.length > 0 || educPills.length > 0) && (
          <div className="px-4 pt-3 pb-2 border-b border-border">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-ink-muted mb-2">Vie quotidienne</p>
            <div className="space-y-2">
              <PillRow label="Commerces" items={commercesPills} />
              <PillRow label="Santé" items={santePills} />
              <PillRow label="Éducation" items={educPills} />
            </div>
          </div>
        )}

        {/* ── Transports ── */}
        {tPills.length > 0 && (
          <div className="px-4 pt-3 pb-2 border-b border-border">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-ink-muted mb-2">Logement & accès</p>
            <div className="space-y-2">
              <PillRow label="Transports" items={tPills} />
            </div>
          </div>
        )}

        {/* ── Cadre de vie ── */}
        {brutes.risques_detail != null && (
          <div className="px-4 pt-3 pb-2 border-b border-border">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-ink-muted mb-2">Cadre de vie</p>
            <div className="space-y-2">
              <PillRow label="Risques" items={riskPills} />
            </div>
          </div>
        )}

        {/* ── Sports & culture ── */}
        {(sportsPills.length > 0 || culturePills.length > 0) && (
          <div className="px-4 pt-3 pb-2">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-ink-muted mb-2">Sports & culture</p>
            <div className="space-y-2">
              <PillRow label="Sports" items={sportsPills} />
              <PillRow label="Culture" items={culturePills} />
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
