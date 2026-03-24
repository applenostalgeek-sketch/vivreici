import { useState, useEffect, useRef } from 'react'

const API_BASE = '/api'
const ADRESSE_API = 'https://api-adresse.data.gouv.fr/search/'

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
        // Recherche communes + adresses en parallèle
        // L'API adresse n'accepte qu'un seul type à la fois — on prend housenumber (numéro) en priorité
        const adresseUrl = `${ADRESSE_API}?q=${encodeURIComponent(query)}&limit=4&type=housenumber`
        const adresseUrlFallback = `${ADRESSE_API}?q=${encodeURIComponent(query)}&limit=4&type=street`

        const [communesResp, adressesResp] = await Promise.allSettled([
          fetch(`${API_BASE}/communes/search?q=${encodeURIComponent(query)}&limit=5`),
          fetch(adresseUrl),
        ])

        const communes = communesResp.status === 'fulfilled' && communesResp.value.ok
          ? (await communesResp.value.json()).map(c => ({ ...c, _type: 'commune' }))
          : []

        let adresses = []
        if (adressesResp.status === 'fulfilled' && adressesResp.value.ok) {
          const geo = await adressesResp.value.json()
          let features = geo.features || []
          // Si aucun résultat housenumber, fallback sur street
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

        // Communes d'abord, puis adresses — on garde TOUTES les adresses
        // (même si la commune est déjà listée : l'adresse donne la précision quartier)
        setResults([...communes, ...adresses])
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

export async function locateByCoords(lat, lng) {
  const resp = await fetch(`${API_BASE}/locate?lat=${lat}&lng=${lng}`)
  if (!resp.ok) throw new Error('Localisation échouée')
  return resp.json()
}

export async function getCommune(codeInsee) {
  const resp = await fetch(`${API_BASE}/communes/${codeInsee}`)
  if (!resp.ok) throw new Error('Commune introuvable')
  return resp.json()
}

export async function getClassement(params = {}) {
  const qs = new URLSearchParams(params).toString()
  const resp = await fetch(`${API_BASE}/classement?${qs}`)
  if (!resp.ok) throw new Error('Erreur classement')
  return resp.json()
}
