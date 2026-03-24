import { useEffect, useRef, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { SCORE_COLORS, SCORE_LABELS, IRIS_ZOOM_THRESHOLD } from '../constants.js'
import { loadCommunes } from '../hooks/useSearch.js'

// En dessous de ce zoom : cercles préchargés. Au-dessus : polygones IRIS.
const POLYGON_ZOOM = 9

function getRadius(population, zoom) {
  const base = Math.log10(Math.max(population || 100, 100)) * 1.5
  const zoomFactor = Math.max(0.5, (zoom - 4) * 0.3)
  return Math.max(3, Math.min(20, base * zoomFactor))
}

function makeTooltip(nom, lettre, score, population) {
  const scoreLabel = score != null
    ? `Score <strong>${lettre}</strong> — ${Math.round(score)}/100`
    : 'Score en cours'
  const popLabel = population > 0
    ? `<br/><span style="color:#888">${population.toLocaleString('fr-FR')} hab.</span>`
    : ''
  return `<strong>${nom}</strong><br/>${scoreLabel}${popLabel}`
}

async function fetchAndBuild(L, url, bboxKeyRef, bboxKey, activeLettersRef, buildFn) {
  const data = await fetch(url).then(r => r.json())
  if (bboxKey !== bboxKeyRef.current) return null  // vue changée pendant le fetch
  const layers = []
  const letters = activeLettersRef.current
  data.forEach(item => {
    if (!item.lettre || !letters.has(item.lettre)) return
    const layer = buildFn(L, item)
    if (layer) layers.push(layer)
  })
  return layers
}

export default function MapView({
  initialCenter = [46.603354, 1.888334],
  initialZoom = 6,
  marker = null,
  className = 'h-full',
}) {
  const mapRef               = useRef(null)
  const leafletMap           = useRef(null)
  const leafletRef           = useRef(null)
  const circleLayerRef       = useRef(null)   // cercles préchargés (zoom < POLYGON_ZOOM)
  const circleMarkersRef     = useRef([])      // [{circle, commune}] pour le filtre
  const communePolyLayerRef  = useRef(null)   // polygones communes (POLYGON_ZOOM <= zoom < IRIS)
  const irisLayerRef         = useRef(null)   // polygones IRIS (zoom >= IRIS_ZOOM_THRESHOLD)
  const markerLayerRef       = useRef(null)   // pin adresse
  const activeLettersRef     = useRef(new Set(['A', 'B', 'C', 'D', 'E']))
  const chargerCommunePolyRef = useRef(null)
  const chargerIrisRef        = useRef(null)
  const lastCommunePolyBbox  = useRef('')
  const lastIrisBbox         = useRef('')
  const navigate             = useNavigate()

  const [irisMode, setIrisMode]             = useState(false)
  const [polyMode, setPolyMode]             = useState(false)
  const [activeLetters, setActiveLetters]   = useState(new Set(['A', 'B', 'C', 'D', 'E']))

  const toggleLetter = useCallback((letter) => {
    setActiveLetters(prev => {
      const next = new Set(prev)
      if (next.has(letter)) { if (next.size === 1) return prev; next.delete(letter) }
      else next.add(letter)
      return next
    })
  }, [])

  // Sync ref + recharge la couche active
  useEffect(() => {
    activeLettersRef.current = activeLetters
    const map = leafletMap.current
    if (!map) return
    const zoom = map.getZoom()
    if (zoom >= IRIS_ZOOM_THRESHOLD) {
      lastIrisBbox.current = ''; chargerIrisRef.current?.()
    } else if (zoom >= POLYGON_ZOOM) {
      lastCommunePolyBbox.current = ''; chargerCommunePolyRef.current?.()
    } else {
      // Cercles : filtre immédiat en mémoire
      circleMarkersRef.current.forEach(({ circle, commune }) => {
        if (activeLetters.has(commune.lettre)) {
          if (!map.hasLayer(circle)) circle.addTo(circleLayerRef.current)
        } else {
          if (circleLayerRef.current?.hasLayer(circle)) circleLayerRef.current.removeLayer(circle)
        }
      })
    }
  }, [activeLetters])

  // Mise à jour du marker pin
  useEffect(() => {
    if (!leafletMap.current || !markerLayerRef.current || !leafletRef.current) return
    const L = leafletRef.current
    markerLayerRef.current.clearLayers()
    if (marker) {
      const pinIcon = L.divIcon({
        html: `<svg viewBox="0 0 24 36" width="24" height="36" xmlns="http://www.w3.org/2000/svg">
          <path d="M12 0C5.4 0 0 5.4 0 12c0 9 12 24 12 24s12-15 12-24C24 5.4 18.6 0 12 0z" fill="#1c1917" stroke="white" stroke-width="1.5"/>
          <circle cx="12" cy="12" r="4.5" fill="white"/>
        </svg>`,
        className: '', iconSize: [24, 36], iconAnchor: [12, 36],
      })
      const m = L.marker([marker.lat, marker.lng], { icon: pinIcon })
      if (marker.label) m.bindTooltip(marker.label)
      m.addTo(markerLayerRef.current)
    }
  }, [marker])

  useEffect(() => {
    import('leaflet').then((L) => {
      if (leafletMap.current) return
      leafletRef.current = L

      const renderer = L.canvas({ padding: 0.5 })
      const map = L.map(mapRef.current, {
        center: initialCenter,
        zoom: initialZoom,
        zoomControl: true,
        renderer,
      })
      leafletMap.current = map

      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        maxZoom: 19,
      }).addTo(map)

      // ── Marker pin adresse ───────────────────────────────────────────────────
      const markerLayer = L.layerGroup().addTo(map)
      markerLayerRef.current = markerLayer
      if (marker) {
        const pinIcon = L.divIcon({
          html: `<svg viewBox="0 0 24 36" width="24" height="36" xmlns="http://www.w3.org/2000/svg">
            <path d="M12 0C5.4 0 0 5.4 0 12c0 9 12 24 12 24s12-15 12-24C24 5.4 18.6 0 12 0z" fill="#1c1917" stroke="white" stroke-width="1.5"/>
            <circle cx="12" cy="12" r="4.5" fill="white"/>
          </svg>`,
          className: '', iconSize: [24, 36], iconAnchor: [12, 36],
        })
        const m = L.marker([marker.lat, marker.lng], { icon: pinIcon })
        if (marker.label) m.bindTooltip(marker.label)
        m.addTo(markerLayer)
      }

      // ── Couche cercles (préchargés) ──────────────────────────────────────────
      const circleLayer = L.layerGroup()
      circleLayerRef.current = circleLayer
      if (initialZoom < POLYGON_ZOOM) circleLayer.addTo(map)

      // Chargement progressif des communes (4 passes par population)
      fetch('/communes-map.json')
        .then(r => r.json())
        .then(communes => {
          const passes = [
            communes.filter(c => c.population > 50000),
            communes.filter(c => c.population > 10000 && c.population <= 50000),
            communes.filter(c => c.population > 2000  && c.population <= 10000),
            communes.filter(c => c.population <= 2000),
          ]
          function renderPass(i) {
            if (i >= passes.length) return
            const zoom = map.getZoom()
            passes[i].forEach(c => {
              if (!c.latitude || !c.longitude) return
              const color = SCORE_COLORS[c.lettre] || '#9CA3AF'
              const circle = L.circleMarker([c.latitude, c.longitude], {
                radius: getRadius(c.population, zoom),
                fillColor: color,
                color: 'rgba(255,255,255,0.6)',
                weight: 1, opacity: 0.9, fillOpacity: 0.75,
                renderer,
              })
              circle.bindTooltip(makeTooltip(c.nom, c.lettre, c.score_global, c.population), { sticky: true })
              circle.on('click', () => navigate(`/commune/${c.code_insee}?tab=detail`))
              circleMarkersRef.current.push({ circle, commune: c })
              if (activeLettersRef.current.has(c.lettre)) circle.addTo(circleLayer)
            })
            setTimeout(() => renderPass(i + 1), i === 0 ? 50 : 200)
          }
          renderPass(0)
        })
        .catch(() => {})

      // ── Couche polygones communes (désactivée — pas de géométrie en statique) ──
      const communePolyLayer = L.layerGroup()
      communePolyLayerRef.current = communePolyLayer
      // Pas de polygones communes en mode statique — les cercles restent actifs
      async function chargerCommunePoly() {}
      chargerCommunePolyRef.current = chargerCommunePoly

      // ── Couche IRIS (bbox) ────────────────────────────────────────────────────
      const irisLayer = L.layerGroup()
      irisLayerRef.current = irisLayer

      async function chargerIris() {
        const b = map.getBounds()
        const bboxKey = [b.getSouth().toFixed(3), b.getNorth().toFixed(3), b.getWest().toFixed(3), b.getEast().toFixed(3)].join(',')
        if (bboxKey === lastIrisBbox.current) return
        lastIrisBbox.current = bboxKey
        try {
          const communes = await loadCommunes()
          const visibles = communes.filter(c =>
            c.latitude >= b.getSouth() && c.latitude <= b.getNorth() &&
            c.longitude >= b.getWest() && c.longitude <= b.getEast()
          )
          const codes = [...new Set(visibles.map(c => c.code_insee))]
          const fetches = await Promise.allSettled(
            codes.map(code => fetch(`/data/iris-map/${code}.json`).then(r => r.ok ? r.json() : null))
          )
          if (bboxKey !== lastIrisBbox.current) return  // vue changée pendant le fetch
          irisLayer.clearLayers()
          const letters = activeLettersRef.current
          for (const result of fetches) {
            if (result.status !== 'fulfilled' || !result.value) continue
            for (const feature of result.value.features) {
              const z = feature.properties
              if (!z.lettre || !letters.has(z.lettre)) continue
              const color = SCORE_COLORS[z.lettre] || '#9CA3AF'
              const typeLabel = z.typ_iris === 'H' ? 'Quartier résidentiel' : z.typ_iris === 'A' ? "Zone d'activité" : z.typ_iris === 'D' ? 'Zone diversifiée' : ''
              const tooltip = makeTooltip(z.nom, z.lettre, z.score_global, z.population) + (typeLabel ? `<br/><em>${typeLabel}</em>` : '')
              const layer = L.geoJSON(feature.geometry, { style: { fillColor: color, color: '#fff', weight: 1.2, opacity: 0.7, fillOpacity: 0.55 } })
              layer.bindTooltip(tooltip, { sticky: true })
              layer.on('click', () => navigate(`/iris/${z.code_iris}?tab=detail`))
              layer.addTo(irisLayer)
            }
          }
        } catch {}
      }
      chargerIrisRef.current = chargerIris

      // ── Gestion zoom ──────────────────────────────────────────────────────────
      let debounceTimer = null

      function mettreAJourVue() {
        const zoom = map.getZoom()

        if (zoom >= IRIS_ZOOM_THRESHOLD) {
          // Mode IRIS
          if (map.hasLayer(communePolyLayer)) communePolyLayer.removeFrom(map)
          if (map.hasLayer(circleLayer)) circleLayer.removeFrom(map)
          if (!map.hasLayer(irisLayer)) irisLayer.addTo(map)
          clearTimeout(debounceTimer)
          debounceTimer = setTimeout(chargerIris, 200)
          setIrisMode(true); setPolyMode(false)
        } else if (zoom >= POLYGON_ZOOM) {
          // Mode intermédiaire — pas de polygones communes en statique, cercles maintenus
          if (map.hasLayer(irisLayer)) { irisLayer.removeFrom(map); irisLayer.clearLayers(); lastIrisBbox.current = '' }
          if (!map.hasLayer(circleLayer)) circleLayer.addTo(map)
          const z = zoom
          circleMarkersRef.current.forEach(({ circle, commune }) => circle.setRadius(getRadius(commune.population, z)))
          setIrisMode(false); setPolyMode(false)
        } else {
          // Mode cercles — toujours préchargés, affichage instantané
          if (map.hasLayer(irisLayer)) { irisLayer.removeFrom(map); irisLayer.clearLayers(); lastIrisBbox.current = '' }
          if (map.hasLayer(communePolyLayer)) { communePolyLayer.removeFrom(map); communePolyLayer.clearLayers(); lastCommunePolyBbox.current = '' }
          if (!map.hasLayer(circleLayer)) circleLayer.addTo(map)
          // Adapter le radius des cercles au zoom
          const z = zoom
          circleMarkersRef.current.forEach(({ circle, commune }) => circle.setRadius(getRadius(commune.population, z)))
          setIrisMode(false); setPolyMode(false)
        }
      }

      map.on('zoomend', mettreAJourVue)
      map.on('moveend', () => {
        clearTimeout(debounceTimer)
        const zoom = map.getZoom()
        if (zoom >= IRIS_ZOOM_THRESHOLD) debounceTimer = setTimeout(chargerIris, 300)
        else if (zoom >= POLYGON_ZOOM)   debounceTimer = setTimeout(chargerCommunePoly, 300)
      })

      // Chargement initial selon le zoom de départ
      if (initialZoom >= IRIS_ZOOM_THRESHOLD) {
        circleLayer.removeFrom(map)
        irisLayer.addTo(map)
        setIrisMode(true)
        chargerIris()
      } else if (initialZoom >= POLYGON_ZOOM) {
        communePolyLayer.addTo(map)
        setPolyMode(true)
        chargerCommunePoly()
      }
      // else: zoom < POLYGON_ZOOM → cercles déjà configurés ci-dessus
    })

    return () => {
      if (leafletMap.current) { leafletMap.current.remove(); leafletMap.current = null }
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const mode = irisMode ? 'Quartiers (IRIS)' : polyMode ? 'Communes' : 'Communes'

  return (
    <div className={`relative ${className}`}>
      <div ref={mapRef} className="w-full h-full" />

      <div className="absolute bottom-4 left-3 z-[1000] bg-white/95 backdrop-blur-sm border border-border rounded-xl p-3 shadow-lg">
        <p className="text-xs font-semibold text-ink mb-2 uppercase tracking-wider">{mode}</p>
        <div className="space-y-1.5">
          {Object.entries(SCORE_COLORS).map(([lettre, color]) => (
            <button
              key={lettre}
              onClick={() => toggleLetter(lettre)}
              className={`flex items-center gap-2 text-xs w-full text-left transition-opacity ${
                activeLetters.has(lettre) ? 'opacity-100' : 'opacity-35'
              }`}
            >
              <span className="w-3 h-3 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
              <span><strong className="text-ink">{lettre}</strong> — {SCORE_LABELS[lettre]}</span>
            </button>
          ))}
        </div>
        {!irisMode && (
          <p className="text-xs text-ink-light mt-2 pt-2 border-t border-border">
            Zoomez pour les quartiers
          </p>
        )}
      </div>
    </div>
  )
}
