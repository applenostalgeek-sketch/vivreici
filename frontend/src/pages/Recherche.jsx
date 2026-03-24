import { useState, useEffect, useRef, useCallback } from 'react'
import { useNavigate, Link, useSearchParams } from 'react-router-dom'
import Nav from '../components/Nav.jsx'
import { SCORE_COLORS, SCORE_LABELS, SCORE_MIN_VALUES, RAYON_OPTIONS } from '../constants.js'
import { scoreToLettre } from '../utils/scoreUtils.js'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { loadCommunes } from '../hooks/useSearch.js'

function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLng = (lng2 - lng1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

async function rechercheGeo({ lat, lng, rayon_km, score_min = 0, min_population = 0, limit = 150 }) {
  const communes = await loadCommunes()
  const results = []
  for (const c of communes) {
    if (!c.latitude || !c.longitude) continue
    if (c.score_global == null) continue
    if (score_min > 0 && c.score_global < score_min) continue
    if (min_population > 0 && (c.population || 0) < min_population) continue
    const dist = haversineKm(lat, lng, c.latitude, c.longitude)
    if (dist > rayon_km) continue
    results.push({
      code_insee: c.code_insee,
      nom: c.nom,
      departement: c.departement,
      population: c.population,
      distance_km: Math.round(dist * 10) / 10,
      score: {
        score_global: c.score_global,
        lettre: c.lettre,
        sous_scores: c.sous_scores,
        donnees_brutes: c.donnees_brutes,
      },
    })
  }
  results.sort((a, b) => (b.score.score_global ?? -1) - (a.score.score_global ?? -1))
  return results.slice(0, limit)
}

// Autocomplete adresse via API Adresse gouv.fr
function useAddressSearch() {
  const [query, setQuery] = useState('')
  const [suggestions, setSuggestions] = useState([])
  const [selected, setSelected] = useState(null)
  const debounceRef = useRef(null)

  const search = useCallback((q) => {
    setQuery(q)
    setSelected(null)
    clearTimeout(debounceRef.current)
    if (q.length < 3) { setSuggestions([]); return }
    debounceRef.current = setTimeout(async () => {
      try {
        const r = await fetch(`https://api-adresse.data.gouv.fr/search/?q=${encodeURIComponent(q)}&limit=6&type=municipality`)
        const data = await r.json()
        setSuggestions(data.features || [])
      } catch {
        setSuggestions([])
      }
    }, 250)
  }, [])

  const pick = useCallback((feature) => {
    setSelected(feature)
    setQuery(feature.properties.label)
    setSuggestions([])
  }, [])

  return { query, suggestions, selected, search, pick }
}

export default function Recherche() {
  usePageMeta({
    title: 'Recherche par rayon',
    description: "Trouvez les communes avec le meilleur score de qualité de vie autour d'une adresse.",
  })

  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { query, suggestions, selected, search, pick } = useAddressSearch()
  const [rayon, setRayon] = useState(() => Number(searchParams.get('rayon') || 20))
  const [scoreMin, setScoreMin] = useState(() => searchParams.get('score') || 'all')
  const [minPop, setMinPop] = useState(() => Number(searchParams.get('pop') || 0))
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(false)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const canSearch = selected != null

  // Pré-remplir et lancer depuis les params URL (ex: liens homepage)
  useEffect(() => {
    const lat = parseFloat(searchParams.get('lat'))
    const lng = parseFloat(searchParams.get('lng'))
    const ville = searchParams.get('ville')
    if (!lat || !lng || !ville) return
    // Pré-remplir le champ adresse
    pick({
      geometry: { coordinates: [lng, lat] },
      properties: { label: ville, name: ville, citycode: '' },
    })
    // Lancer la recherche directement
    setLoading(true)
    setResults(null)
    rechercheGeo({ lat, lng, rayon_km: rayon, score_min: SCORE_MIN_VALUES[scoreMin] ?? 0, min_population: minPop, limit: 150 })
      .then(setResults)
      .catch(() => setResults([]))
      .finally(() => setLoading(false))
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const doSearch = useCallback(async () => {
    if (!selected) return
    const [lng, lat] = selected.geometry.coordinates
    setLoading(true)
    setResults(null)
    // Persist params in URL for back-navigation
    setSearchParams({ rayon, score: scoreMin, pop: minPop }, { replace: true })
    try {
      const data = await rechercheGeo({
        lat, lng,
        rayon_km: rayon,
        score_min: SCORE_MIN_VALUES[scoreMin] ?? 0,
        min_population: minPop,
        limit: 150,
      })
      setResults(data)
    } catch {
      setResults([])
    } finally {
      setLoading(false)
    }
  }, [selected, rayon, scoreMin, minPop])

  const [modeLocal, setModeLocal] = useState(true)

  // Recalcule score_immobilier en percentile LOCAL parmi les résultats
  function localiserImmo(results) {
    const prix = results
      .map(r => r.score?.donnees_brutes?.prix_m2_median ?? 0)
      .filter(p => p > 0)
    if (prix.length < 3) return results  // pas assez de données
    prix.sort((a, b) => a - b)
    return results.map(r => {
      const p = r.score?.donnees_brutes?.prix_m2_median
      if (!p || p <= 0) return r
      // percentile inverse : moins cher = meilleur score
      const rank = prix.filter(x => x < p).length / prix.length
      const scoreImmoLocal = Math.round((1 - rank) * 100)
      const sousCopy = { ...r.score.sous_scores, immobilier: scoreImmoLocal }
      const cats = ['equipements', 'securite', 'immobilier', 'education', 'sante', 'revenus', 'transports']
      const weights_def = { equipements: 0.18, securite: 0.18, immobilier: 0.14, education: 0.14, sante: 0.14, revenus: 0.14, transports: 0.10 }
      let wsum = 0, vsum = 0
      for (const cat of cats) {
        const v = sousCopy[cat]
        if (v != null && v >= 0) { vsum += v * weights_def[cat]; wsum += weights_def[cat] }
      }
      const scoreLocal = wsum > 0 ? Math.round(vsum / wsum) : r.score.score_global
      return { ...r, _scoreLocal: scoreLocal, _immoLocal: scoreImmoLocal }
    })
  }

  const localizedResults = results ? localiserImmo(results) : null

  function effectiveScore(commune) {
    if (!commune.score) return 0
    if (modeLocal && commune._scoreLocal != null) return commune._scoreLocal
    return commune.score.score_global
  }
  function effectiveLettre(commune) {
    const s = effectiveScore(commune)
    return scoreToLettre(s)
  }

  const sortedResults = localizedResults
    ? [...localizedResults].sort((a, b) => effectiveScore(b) - effectiveScore(a))
    : null

  return (
    <div className="min-h-screen">
      <Nav />

      <main className="max-w-4xl mx-auto px-6 py-12">
        <div className="mb-10">
          <h1 className="font-display text-4xl md:text-5xl text-ink mb-3">
            Où habiter près de <em className="not-italic text-score-A">chez vous ?</em>
          </h1>
          <p className="text-ink-light text-lg max-w-xl">
            Trouvez les communes avec le meilleur score de qualité de vie dans un rayon donné autour d'une adresse.
          </p>
        </div>

        {/* Search form */}
        <div className="bg-white border border-border rounded-2xl p-6 mb-8 space-y-6">
          {/* Address input */}
          <div>
            <label className="block text-sm font-medium text-ink mb-2">Adresse de référence</label>
            <div className="relative">
              <input
                type="text"
                value={query}
                onChange={e => { search(e.target.value); setShowSuggestions(true) }}
                onFocus={() => setShowSuggestions(true)}
                onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
                placeholder="Ex. : 10 rue de la Paix, Paris…"
                className="w-full border border-border rounded-xl px-4 py-3 text-sm bg-white text-ink placeholder:text-ink-light focus:outline-none focus:border-ink/40 focus:ring-1 focus:ring-ink/20"
              />
              {selected && (
                <span className="absolute right-3 top-1/2 -translate-y-1/2 text-score-A text-sm">✓</span>
              )}
              {showSuggestions && suggestions.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-border rounded-xl shadow-lg z-50 overflow-hidden">
                  {suggestions.map(feat => (
                    <button
                      key={feat.properties.id}
                      onMouseDown={() => pick(feat)}
                      className="w-full text-left px-4 py-2.5 text-sm hover:bg-paper transition-colors border-b border-border/50 last:border-0"
                    >
                      <span className="font-medium text-ink">{feat.properties.name}</span>
                      <span className="text-ink-light ml-2 text-xs">{feat.properties.context}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Rayon + score min */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-ink mb-2">
                Rayon : <span className="text-score-A font-semibold">{rayon} km</span>
              </label>
              <input
                type="range"
                min="5" max="50" step="5"
                value={rayon}
                onChange={e => setRayon(Number(e.target.value))}
                className="w-full accent-ink"
              />
              <div className="flex justify-between text-xs text-ink-light mt-1">
                {RAYON_OPTIONS.map(r => <span key={r}>{r}</span>)}
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-ink mb-2">Score minimum</label>
              <select
                value={scoreMin}
                onChange={e => setScoreMin(e.target.value)}
                className="w-full border border-border rounded-xl px-3 py-2.5 text-sm bg-white text-ink focus:outline-none focus:border-ink/40"
              >
                <option value="all">Toutes les communes</option>
                {Object.entries(SCORE_LABELS).map(([k, label]) => (
                  <option key={k} value={k}>{label} minimum</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-ink mb-2">Population minimum</label>
              <select
                value={minPop}
                onChange={e => setMinPop(Number(e.target.value))}
                className="w-full border border-border rounded-xl px-3 py-2.5 text-sm bg-white text-ink focus:outline-none focus:border-ink/40"
              >
                <option value={0}>Toutes tailles</option>
                <option value={500}>500+ hab.</option>
                <option value={2000}>2 000+ hab.</option>
                <option value={5000}>5 000+ hab.</option>
                <option value={10000}>10 000+ hab.</option>
              </select>
            </div>
          </div>

          <p className="text-xs text-ink-light bg-amber-50 border border-amber-100 rounded-xl px-4 py-2.5">
            <strong className="text-ink">Note :</strong> Le score immobilier national défavorise les zones chères (Paris, Lyon…).
            Activez le <strong>score local</strong> dans les résultats pour comparer les communes entre elles.
          </p>

          <button
            onClick={doSearch}
            disabled={!canSearch || loading}
            className="w-full bg-ink text-white rounded-xl px-6 py-3 font-medium text-sm hover:bg-ink/80 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {loading ? 'Recherche en cours…' : `Trouver les meilleures communes dans ${rayon} km`}
          </button>
        </div>

        {/* Results */}
        {loading && (
          <div className="flex items-center justify-center py-16 text-ink-light gap-3">
            <div className="w-5 h-5 border-2 border-border border-t-ink rounded-full animate-spin" />
            Calcul en cours…
          </div>
        )}

        {sortedResults && (
          <div>
            <div className="flex items-center justify-between mb-4">
              <div>
                <h2 className="font-display text-xl text-ink">
                  {sortedResults.length} commune{sortedResults.length !== 1 ? 's' : ''} trouvée{sortedResults.length !== 1 ? 's' : ''}
                </h2>
                <p className="text-sm text-ink-light mt-0.5">
                  dans {rayon} km autour de{' '}
                  <strong className="text-ink">{selected?.properties?.name}</strong>
                </p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setModeLocal(m => !m)}
                  className={`text-xs rounded-full px-3 py-1.5 border transition-colors ${
                    modeLocal
                      ? 'bg-ink text-white border-ink'
                      : 'bg-white text-ink-light border-border hover:border-ink/40'
                  }`}
                  title="Le score immobilier est recalculé localement (par rapport aux communes de la liste) plutôt que nationalement"
                >
                  {modeLocal ? 'Score local actif' : 'Score national'}
                </button>
              </div>
            </div>

            {sortedResults.length === 0 ? (
              <div className="text-center py-16 bg-white border border-border rounded-2xl text-ink-light">
                <p className="text-lg mb-2">Aucune commune trouvée</p>
                <p className="text-sm">Essayez d'augmenter le rayon ou d'abaisser le score minimum.</p>
              </div>
            ) : (
              <div className="space-y-2">
                {sortedResults.map((commune, i) => {
                  const lettre = effectiveLettre(commune)
                  const score = effectiveScore(commune)
                  const color = SCORE_COLORS[lettre] || '#9CA3AF'
                  const topLocal = i < Math.max(1, Math.ceil(sortedResults.length * 0.10))
                  const localLabel = i === 0 ? 'Meilleur du secteur'
                    : topLocal ? `Top ${Math.ceil(sortedResults.length * 0.10)} local`
                    : null
                  return (
                    <button
                      key={commune.code_insee}
                      onClick={() => navigate(`/commune/${commune.code_insee}`)}
                      className="w-full flex items-center gap-4 bg-white border border-border rounded-xl px-5 py-3.5 hover:border-ink/40 transition-all text-left group"
                    >
                      {/* Rank */}
                      <span className="font-mono text-sm text-ink-light w-6 text-center flex-shrink-0">
                        {i + 1}
                      </span>

                      {/* Name + info */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-ink group-hover:underline truncate">{commune.nom}</span>
                          {localLabel && (
                            <span className="text-xs text-score-A font-medium flex-shrink-0 hidden sm:inline">{localLabel}</span>
                          )}
                        </div>
                        <div className="flex items-center gap-3 text-xs text-ink-light mt-0.5">
                          {commune.departement && <span>Dept. {commune.departement}</span>}
                          {commune.population > 0 && (
                            <span>{commune.population.toLocaleString('fr-FR')} hab.</span>
                          )}
                        </div>
                      </div>

                      {/* Distance */}
                      <div className="text-center flex-shrink-0 hidden sm:block">
                        <div className="font-mono text-sm font-semibold text-ink">{commune.distance_km} km</div>
                        <div className="text-xs text-ink-light">de l'adresse</div>
                      </div>

                      {/* Score */}
                      <div className="flex items-center gap-2 flex-shrink-0">
                        <span className="font-mono text-sm text-ink-light hidden md:inline">{Math.round(score)}</span>
                        <div
                          className="w-9 h-9 rounded-xl flex items-center justify-center font-display font-bold text-white text-base"
                          style={{ backgroundColor: color }}
                        >
                          {lettre}
                        </div>
                      </div>
                    </button>
                  )
                })}
              </div>
            )}
          </div>
        )}
      </main>
      <footer className="border-t border-border px-6 py-6 text-center text-xs text-ink-light">
        VivreIci · BPE 2024 INSEE · DVF 2024 DGFiP · SSMSI 2024 · APL 2023 DREES · CEREMA 2023 · Filosofi 2021 INSEE ·{' '}
        <Link to="/methode" className="underline hover:text-ink">Méthode</Link>
      </footer>
    </div>
  )
}
