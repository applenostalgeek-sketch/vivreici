import { useState, useEffect, useRef } from 'react'

const ADRESSE_API = 'https://api-adresse.data.gouv.fr/search/'

// Cache singleton communes-map.json
let communesPromise = null
export function loadCommunes() {
  if (!communesPromise) {
    communesPromise = fetch('/communes-map.json').then(r => r.json())
  }
  return communesPromise
}

// Cache singleton iris-locator.json
let irisLocatorPromise = null
function loadIrisLocator() {
  if (!irisLocatorPromise) {
    irisLocatorPromise = fetch('/iris-locator.json').then(r => r.json())
  }
  return irisLocatorPromise
}

function normalize(s) {
  return (s || '').toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/['-]/g, ' ')
    .trim()
}

function searchCommunes(communes, query, limit = 5) {
  const q = normalize(query)
  if (!q) return []
  const scored = []
  for (const c of communes) {
    const nom = normalize(c.nom)
    const dep = normalize(c.departement || '')
    const cp = (c.codes_postaux || []).join(' ')
    if (nom.startsWith(q) || cp.startsWith(q)) {
      scored.push([0, c])
    } else if (nom.includes(q) || dep.includes(q) || cp.includes(q)) {
      scored.push([1, c])
    }
  }
  scored.sort((a, b) => {
    if (a[0] !== b[0]) return a[0] - b[0]
    return (b[1].population || 0) - (a[1].population || 0)
  })
  return scored.slice(0, limit).map(([, c]) => c)
}

export function useSearch() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const debounceRef = useRef(null)

  useEffect(() => {
    if (query.length < 2) {
      setResults([])
      return
    }

    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(async () => {
      setLoading(true)
      try {
        const adresseUrl = `${ADRESSE_API}?q=${encodeURIComponent(query)}&limit=4&type=housenumber`
        const adresseUrlFallback = `${ADRESSE_API}?q=${encodeURIComponent(query)}&limit=4&type=street`

        const [communesResult, adressesResp] = await Promise.allSettled([
          loadCommunes(),
          fetch(adresseUrl),
        ])

        const communeResults = communesResult.status === 'fulfilled'
          ? searchCommunes(communesResult.value, query, 5).map(c => ({
              ...c,
              _type: 'commune',
              score: { lettre: c.lettre, score_global: c.score_global },
            }))
          : []

        let adresses = []
        if (adressesResp.status === 'fulfilled' && adressesResp.value.ok) {
          const geo = await adressesResp.value.json()
          let features = geo.features || []
          if (features.length === 0) {
            try {
              const fallbackResp = await fetch(adresseUrlFallback)
              if (fallbackResp.ok) {
                const fallbackGeo = await fallbackResp.json()
                features = fallbackGeo.features || []
              }
            } catch { /* ignore */ }
          }
          adresses = features.map(f => ({
            _type: 'adresse',
            label: f.properties.label,
            lat: f.geometry.coordinates[1],
            lng: f.geometry.coordinates[0],
            code_insee: f.properties.citycode,
            city: f.properties.city,
          }))
        }

        setResults([...communeResults, ...adresses])
      } catch {
        setResults([])
      } finally {
        setLoading(false)
      }
    }, 250)

    return () => clearTimeout(debounceRef.current)
  }, [query])

  return { query, setQuery, results, loading }
}

function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLng = (lng2 - lng1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

export async function locateByCoords(lat, lng) {
  const locator = await loadIrisLocator()
  // locator = [[code_iris, code_commune, lat, lng], ...]
  let best = null
  let bestDist = Infinity
  for (const [codeIris, codeCommune, iLat, iLng] of locator) {
    const d = haversineKm(lat, lng, iLat, iLng)
    if (d < bestDist) {
      bestDist = d
      best = { code_iris: codeIris, code_commune: codeCommune }
    }
  }
  if (!best) throw new Error('Aucun IRIS trouvé')
  return best
}

export async function getCommune(codeInsee) {
  const resp = await fetch(`/data/communes/${codeInsee}.json`)
  if (!resp.ok) throw new Error('Commune introuvable')
  return resp.json()
}

export async function getClassement(params = {}) {
  const communes = await loadCommunes()
  const { departement, min_population = 0, limit = 100, ordre = 'desc', offset = 0 } = params

  // Filtres par score minimum par catégorie (ex: securite_min, sante_min, ...)
  const catMins = {}
  for (const [k, v] of Object.entries(params)) {
    if (k.endsWith('_min') && v != null) {
      catMins[k.replace('_min', '')] = Number(v)
    }
  }

  let filtered = communes.filter(c => {
    if (c.score_global == null) return false
    if (min_population > 0 && (c.population || 0) < min_population) return false
    if (departement && c.departement !== departement) return false
    for (const [cat, minVal] of Object.entries(catMins)) {
      const v = c.sous_scores?.[cat]
      if (v == null || v < minVal) return false
    }
    return true
  })
  filtered.sort((a, b) => {
    const va = a.score_global ?? -1
    const vb = b.score_global ?? -1
    return ordre === 'asc' ? va - vb : vb - va
  })
  return filtered.slice(Number(offset), Number(offset) + Number(limit)).map(c => ({
    code_insee: c.code_insee,
    nom: c.nom,
    departement: c.departement,
    region: c.region,
    population: c.population,
    score: {
      score_global: c.score_global,
      lettre: c.lettre,
      sous_scores: c.sous_scores,
    },
  }))
}
