import { useEffect, useRef, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { SCORE_COLORS, SCORE_LABELS, IRIS_ZOOM_THRESHOLD } from '../constants.js'
import { loadCommunes, loadCommunesScores } from '../hooks/useSearch.js'
import { useProfile } from '../context/ProfileContext.jsx'
import { PROFILES } from '../utils/profiles.js'

// En dessous de ce zoom : cercles préchargés. Au-dessus : polygones IRIS.
const POLYGON_ZOOM = 9

// ── Helpers profil (pas de dépendance React) ──────────────────────────────────
function _scoreToLettre(s) {
  return s >= 80 ? 'A' : s >= 60 ? 'B' : s >= 40 ? 'C' : s >= 20 ? 'D' : 'E'
}

function _computeProfileScore(sous_scores, weights) {
  if (!sous_scores || !weights) return null
  const available = {}
  for (const [cat, w] of Object.entries(weights)) {
    const val = sous_scores[cat]
    if (val != null && val >= 0) available[cat] = val
  }
  if (Object.keys(available).length === 0) return null
  const totalW = Object.keys(available).reduce((s, cat) => s + weights[cat], 0)
  const score = Object.entries(available).reduce(
    (s, [cat, val]) => s + val * (weights[cat] / totalW), 0
  )
  return Math.round(score * 10) / 10
}

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
  const circleLayerRef       = useRef(null)
  const circleMarkersRef     = useRef([])
  const deptLayerRef         = useRef(null)
  const renderDeptsRef       = useRef(null)
  const communePolyLayerRef  = useRef(null)
  const irisLayerRef         = useRef(null)
  const markerLayerRef       = useRef(null)
  const activeLettersRef     = useRef(new Set(['A', 'B', 'C', 'D', 'E']))
  const chargerCommunePolyRef = useRef(null)
  const chargerIrisRef        = useRef(null)
  const lastCommunePolyBbox  = useRef('')
  const lastIrisBbox         = useRef('')

  // Profil
  const profileRef           = useRef('national')
  const scoresMapRef         = useRef(null)          // Map<code_insee, {sous_scores}>
  const deptProfileRef       = useRef(null)          // Map<dept, {score_median, lettre}> pour profil

  const navigate             = useNavigate()
  const { profile }          = useProfile()

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

  // Sync activeLetters ref + recharge la couche active
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
      renderDeptsRef.current?.()
    }
  }, [activeLetters])

  // Sync profil → pré-calcule les scores profil + recharge les couches
  useEffect(() => {
    profileRef.current = profile
    const map = leafletMap.current
    if (!map) return

    async function updateForProfile() {
      const weights = profile !== 'national' ? PROFILES[profile]?.weights : null

      if (weights) {
        const [communes, sMap] = await Promise.all([loadCommunes(), loadCommunesScores()])
        scoresMapRef.current = sMap

        // Pré-calcul scores profil par département (médiane)
        const deptBuckets = {}
        for (const c of communes) {
          const dept = c.departement
          if (!dept) continue
          const s = sMap.get(c.code_insee)
          if (!s?.sous_scores) continue
          const ps = _computeProfileScore(s.sous_scores, weights)
          if (ps == null) continue
          if (!deptBuckets[dept]) deptBuckets[dept] = []
          deptBuckets[dept].push(ps)
        }
        const deptResult = {}
        for (const [dept, scores] of Object.entries(deptBuckets)) {
          const sorted = [...scores].sort((a, b) => a - b)
          const median = sorted[Math.floor(sorted.length / 2)]
          deptResult[dept] = { score_median: median, lettre: _scoreToLettre(median) }
        }
        deptProfileRef.current = deptResult
      } else {
        scoresMapRef.current = null
        deptProfileRef.current = null
      }

      // Rafraîchir la couche active
      const zoom = map.getZoom()
      if (zoom >= IRIS_ZOOM_THRESHOLD) {
        lastIrisBbox.current = ''; chargerIrisRef.current?.()
      } else if (zoom >= POLYGON_ZOOM) {
        lastCommunePolyBbox.current = ''; chargerCommunePolyRef.current?.()
      } else {
        renderDeptsRef.current?.()
      }
    }

    updateForProfile()
  }, [profile])

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

      // ── Couche cercles (conservés pour rollback) ─────────────────────────────
      const circleLayer = L.layerGroup()
      circleLayerRef.current = circleLayer

      // ── Couche polygones départements (zoom < POLYGON_ZOOM) ─────────────────
      const deptLayer = L.layerGroup()
      deptLayerRef.current = deptLayer
      if (initialZoom < POLYGON_ZOOM) deptLayer.addTo(map)

      let deptData = null

      function renderDepts() {
        if (!deptData) return
        deptLayer.clearLayers()
        const letters = activeLettersRef.current
        const deptProfile = deptProfileRef.current  // null si profil national

        for (const feature of deptData.features) {
          const p = feature.properties

          // Score à afficher : profil si dispo, sinon national
          const pd = deptProfile?.[p.dept]
          const lettre = pd?.lettre || p.lettre
          const scoreMedian = pd?.score_median ?? p.score_median

          if (!lettre) {
            const layer = L.geoJSON(feature.geometry, { style: { fillColor: '#CBD5E1', color: '#fff', weight: 1, opacity: 0.5, fillOpacity: 0.35 } })
            layer.bindTooltip(`<strong>Dép. ${p.dept}</strong><br/><span style="color:#888">Données insuffisantes</span>`, { sticky: true })
            layer.addTo(deptLayer)
            continue
          }
          if (!letters.has(lettre)) continue
          const color = SCORE_COLORS[lettre] || '#9CA3AF'
          const layer = L.geoJSON(feature.geometry, { style: { fillColor: color, color: '#fff', weight: 1, opacity: 0.6, fillOpacity: 0.55 } })
          layer.bindTooltip(`<strong>Dép. ${p.dept}</strong><br/>Score médian : ${Math.round(scoreMedian)}/100 (${p.nb_scorees} communes)`, { sticky: true })
          layer.addTo(deptLayer)
        }
      }
      renderDeptsRef.current = renderDepts

      fetch('/data/departements.json').then(r => r.json()).then(fc => {
        deptData = fc
        if (map.hasLayer(deptLayer)) renderDepts()
      }).catch(() => {})

      // ── Couche polygones communes (zoom intermédiaire POLYGON_ZOOM..IRIS_ZOOM) ──
      const communePolyLayer = L.layerGroup()
      communePolyLayerRef.current = communePolyLayer
      const deptCache = {}

      async function chargerCommunePoly() {
        const b = map.getBounds()
        const bboxKey = [b.getSouth().toFixed(3), b.getNorth().toFixed(3), b.getWest().toFixed(3), b.getEast().toFixed(3)].join(',')
        if (bboxKey === lastCommunePolyBbox.current) return
        lastCommunePolyBbox.current = bboxKey
        try {
          const communes = await loadCommunes()
          const visibles = communes.filter(c =>
            c.latitude >= b.getSouth() && c.latitude <= b.getNorth() &&
            c.longitude >= b.getWest() && c.longitude <= b.getEast()
          )
          const depts = [...new Set(visibles.map(c => c.departement).filter(Boolean))]
          const toFetch = depts.filter(d => !deptCache[d])
          if (toFetch.length > 0) {
            const fetches = await Promise.allSettled(
              toFetch.map(dept => fetch(`/data/communes-geo/${dept}.json`).then(r => r.ok ? r.json() : null))
            )
            if (bboxKey !== lastCommunePolyBbox.current) return
            toFetch.forEach((dept, i) => {
              if (fetches[i].status === 'fulfilled' && fetches[i].value) deptCache[dept] = fetches[i].value
            })
          } else if (bboxKey !== lastCommunePolyBbox.current) return

          communePolyLayer.clearLayers()
          const letters = activeLettersRef.current
          const sMap = scoresMapRef.current
          const weights = profileRef.current !== 'national' ? PROFILES[profileRef.current]?.weights : null

          for (const dept of depts) {
            const fc = deptCache[dept]
            if (!fc) continue
            for (const feature of fc.features) {
              const p = feature.properties

              // Score profil si dispo
              const ss = sMap?.get(p.code_insee)?.sous_scores
              const ps = weights && ss ? _computeProfileScore(ss, weights) : null
              const lettre = ps != null ? _scoreToLettre(ps) : p.lettre
              const scoreGlobal = ps ?? p.score_global

              if (!lettre) {
                const layer = L.geoJSON(feature.geometry, { style: { fillColor: '#CBD5E1', color: '#fff', weight: 0.8, opacity: 0.5, fillOpacity: 0.35 } })
                layer.bindTooltip(`<strong>${p.nom}</strong><br/><span style="color:#888">Données insuffisantes</span>`, { sticky: true })
                layer.on('click', () => navigate(`/commune/${p.code_insee}?tab=detail`))
                layer.addTo(communePolyLayer)
                continue
              }
              if (!letters.has(lettre)) continue
              const color = SCORE_COLORS[lettre] || '#9CA3AF'
              const layer = L.geoJSON(feature.geometry, { style: { fillColor: color, color: '#fff', weight: 0.8, opacity: 0.6, fillOpacity: 0.55 } })
              layer.bindTooltip(makeTooltip(p.nom, lettre, scoreGlobal, p.population), { sticky: true })
              layer.on('click', () => navigate(`/commune/${p.code_insee}?tab=detail`))
              layer.addTo(communePolyLayer)
            }
          }
        } catch {}
      }
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
          if (bboxKey !== lastIrisBbox.current) return
          irisLayer.clearLayers()
          const letters = activeLettersRef.current
          const sMap = scoresMapRef.current
          const weights = profileRef.current !== 'national' ? PROFILES[profileRef.current]?.weights : null

          for (let i = 0; i < fetches.length; i++) {
            const result = fetches[i]
            if (result.status !== 'fulfilled' || !result.value) continue
            for (const feature of result.value.features) {
              const z = feature.properties
              const isZ = z.typ_iris === 'Z'
              const commune = isZ ? visibles.find(c => c.code_insee === z.code_iris.slice(0, 5)) : null

              // Score national de référence pour cet IRIS
              const nationalLettre = isZ ? (commune?.lettre || z.lettre) : z.lettre
              const nationalScore  = isZ ? (commune?.score_global ?? z.score_global) : z.score_global

              // Score profil : uniquement si l'IRIS a déjà un score national
              // (évite d'inventer un score pour les IRIS sans données locales)
              const communeCode = z.code_iris.slice(0, 5)
              const ss = sMap?.get(communeCode)?.sous_scores
              const ps = weights && ss && nationalLettre ? _computeProfileScore(ss, weights) : null

              const lettre = ps != null ? _scoreToLettre(ps) : nationalLettre
              const scoreGlobal = ps ?? nationalScore

              if (!lettre) {
                const layer = L.geoJSON(feature.geometry, { style: { fillColor: '#CBD5E1', color: '#fff', weight: 1, opacity: 0.65, fillOpacity: 0.45 } })
                layer.bindTooltip(`<strong>${z.nom}</strong><br/><span style="color:#888">Données insuffisantes</span>`, { sticky: true })
                const dest = isZ ? `/commune/${z.code_iris.slice(0, 5)}?tab=detail` : `/iris/${z.code_iris}?tab=detail`
                layer.on('click', () => navigate(dest))
                layer.addTo(irisLayer)
                continue
              }

              if (!letters.has(lettre)) continue
              const color = SCORE_COLORS[lettre] || '#9CA3AF'
              const typeLabel = z.typ_iris === 'H' ? 'Quartier résidentiel' : z.typ_iris === 'A' ? "Zone d'activité" : z.typ_iris === 'D' ? 'Zone diversifiée' : ''
              const tooltip = makeTooltip(z.nom, lettre, scoreGlobal, z.population || commune?.population) + (typeLabel ? `<br/><em>${typeLabel}</em>` : '')
              const layer = L.geoJSON(feature.geometry, { style: { fillColor: color, color: '#fff', weight: 1.2, opacity: 0.7, fillOpacity: 0.55 } })
              layer.bindTooltip(tooltip, { sticky: true })
              const dest = isZ
                ? `/commune/${z.code_iris.slice(0, 5)}?tab=detail`
                : `/iris/${z.code_iris}?tab=detail`
              layer.on('click', () => navigate(dest))
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
          if (map.hasLayer(deptLayer)) deptLayer.removeFrom(map)
          if (map.hasLayer(communePolyLayer)) communePolyLayer.removeFrom(map)
          if (!map.hasLayer(irisLayer)) irisLayer.addTo(map)
          clearTimeout(debounceTimer)
          debounceTimer = setTimeout(chargerIris, 200)
          setIrisMode(true); setPolyMode(false)
        } else if (zoom >= POLYGON_ZOOM) {
          if (map.hasLayer(irisLayer)) { irisLayer.removeFrom(map); irisLayer.clearLayers(); lastIrisBbox.current = '' }
          if (map.hasLayer(deptLayer)) deptLayer.removeFrom(map)
          if (!map.hasLayer(communePolyLayer)) communePolyLayer.addTo(map)
          lastCommunePolyBbox.current = ''
          clearTimeout(debounceTimer)
          debounceTimer = setTimeout(chargerCommunePoly, 200)
          setIrisMode(false); setPolyMode(true)
        } else {
          if (map.hasLayer(irisLayer)) { irisLayer.removeFrom(map); irisLayer.clearLayers(); lastIrisBbox.current = '' }
          if (map.hasLayer(communePolyLayer)) { communePolyLayer.removeFrom(map); communePolyLayer.clearLayers(); lastCommunePolyBbox.current = '' }
          if (!map.hasLayer(deptLayer)) deptLayer.addTo(map)
          renderDepts()
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

      if (initialZoom >= IRIS_ZOOM_THRESHOLD) {
        irisLayer.addTo(map)
        setIrisMode(true)
        chargerIris()
      } else if (initialZoom >= POLYGON_ZOOM) {
        if (map.hasLayer(deptLayer)) deptLayer.removeFrom(map)
        communePolyLayer.addTo(map)
        setPolyMode(true)
        chargerCommunePoly()
      }
    })

    return () => {
      if (leafletMap.current) { leafletMap.current.remove(); leafletMap.current = null }
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const modeLabel = irisMode ? 'Vue quartiers' : polyMode ? 'Vue communes' : 'Vue départements'
  const modeHint  = irisMode
    ? null
    : polyMode
    ? 'Zoomez pour voir les quartiers'
    : 'Zoomez pour accéder aux fiches communes'

  const isCustomProfile = profile !== 'national'
  const profileLabel = isCustomProfile ? `${PROFILES[profile].emoji} ${PROFILES[profile].label}` : null

  return (
    <div className={`relative ${className}`}>
      <div ref={mapRef} className="w-full h-full" />

      <div className="absolute bottom-4 left-3 z-[1000] bg-white/95 backdrop-blur-sm border border-border rounded-xl p-3 shadow-lg">
        <div className="flex items-center justify-between gap-3 mb-2">
          <p className="text-xs font-semibold text-ink uppercase tracking-wider">{modeLabel}</p>
          {profileLabel && (
            <span className="text-xs font-medium text-paper bg-ink rounded-full px-2 py-0.5 whitespace-nowrap">
              {profileLabel}
            </span>
          )}
        </div>
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
        {modeHint && (
          <p className="text-xs text-ink-light mt-2 pt-2 border-t border-border">
            {modeHint}
          </p>
        )}
      </div>
    </div>
  )
}
