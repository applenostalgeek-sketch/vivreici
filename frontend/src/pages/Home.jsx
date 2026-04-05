import { useRef, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import Nav from '../components/Nav.jsx'
import MapView from '../components/MapView.jsx'
import SearchBar from '../components/SearchBar.jsx'
import CommunePanel from '../components/CommunePanel.jsx'
import IrisPanel from '../components/IrisPanel.jsx'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { locateByCoords } from '../hooks/useSearch.js'

export default function Home() {
  const { codeInsee, codeIris } = useParams()
  const navigate = useNavigate()
  const mapRef = useRef(null)

  // Déterminer le mode panneau
  const panelType = codeIris ? 'iris' : codeInsee ? 'commune' : null
  const panelCode = codeIris || codeInsee || null

  usePageMeta({
    title: panelType ? undefined : null, // panels set their own title
    description: panelType ? undefined : 'Trouvez où il fait bon vivre en France. Score A à E pour 35 000 communes — équipements, sécurité, immobilier, santé, éducation, transports.',
  })

  // Quand on clique sur la carte
  const handleMapSelect = useCallback(({ type, code }) => {
    if (type === 'commune') navigate(`/commune/${code}`)
    else if (type === 'iris') navigate(`/iris/${code}`)
  }, [navigate])

  // Quand on sélectionne dans la recherche → zoom la carte
  const handleSearch = useCallback(async (item) => {
    if (item._type === 'adresse') {
      // Adresse → trouver le code IRIS et zoomer
      try {
        const loc = await locateByCoords(item.lat, item.lng, item.code_insee)
        if (loc.code_iris) {
          mapRef.current?.flyTo(item.lat, item.lng, 15)
          navigate(`/iris/${loc.code_iris}`)
        } else {
          const code = loc.code_commune || item.code_insee
          if (code) {
            mapRef.current?.flyTo(item.lat, item.lng, 13)
            navigate(`/commune/${code}`)
          }
        }
      } catch {
        if (item.code_insee) {
          mapRef.current?.flyTo(item.lat, item.lng, 13)
          navigate(`/commune/${item.code_insee}`)
        }
      }
    } else {
      // Commune → zoomer sur la commune
      if (item.latitude && item.longitude) {
        mapRef.current?.flyTo(item.latitude, item.longitude, 13)
      }
      navigate(`/commune/${item.code_insee}`)
    }
  }, [navigate])

  const closePanel = useCallback(() => {
    navigate('/')
    mapRef.current?.resetView()
  }, [navigate])

  return (
    <div className="h-[100dvh] flex flex-col overflow-hidden relative">

      {/* ── MAP ────────────────────────────────────────────── */}
      <div className="absolute inset-0 z-0">
        <MapView ref={mapRef} className="h-full w-full" onSelect={handleMapSelect} />
      </div>

      {/* ── NAV ────────────────────────────────────────────── */}
      <Nav overlay onLogoClick={closePanel} />

      {/* ── SEARCH BAR (flottant) ──────────────────────────── */}
      {/* Desktop : centré en bas. Mobile : centré en bas au-dessus du panneau */}
      <div className={`absolute z-[1001] left-1/2 -translate-x-1/2 w-[calc(100%-2rem)] max-w-md transition-all duration-300 ${
        panelType
          ? 'bottom-3 sm:bottom-6 sm:left-[calc(25%-200px)] sm:translate-x-0'
          : 'bottom-6 sm:bottom-8'
      }`}>
        <SearchBar size="sm" placeholder="Commune ou adresse…" onSelect={handleSearch} dropUp />
      </div>

      {/* ── PANNEAU DÉTAIL ─────────────────────────────────── */}
      {/* Desktop : side panel à droite. Mobile : bottom sheet */}
      {panelType && (
        <>
          {/* Overlay clic pour fermer (mobile) */}
          <div
            className="sm:hidden absolute inset-0 z-[1001] bg-black/10"
            onClick={closePanel}
          />

          {/* Desktop side panel */}
          <div className="hidden sm:flex absolute top-0 right-0 z-[1002] h-full w-[420px] max-w-[50vw] flex-col bg-white border-l border-border shadow-2xl animate-slide-in-right">
            <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-paper flex-shrink-0">
              <span className="text-xs font-semibold text-ink-muted uppercase tracking-wider">
                {panelType === 'iris' ? 'Quartier' : 'Commune'}
              </span>
              <button onClick={closePanel} className="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-border transition-colors text-ink-light hover:text-ink">
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-4">
              {panelType === 'commune' && <CommunePanel codeInsee={panelCode} onClose={closePanel} />}
              {panelType === 'iris' && <IrisPanel codeIris={panelCode} onClose={closePanel} />}
            </div>
          </div>

          {/* Mobile bottom sheet */}
          <div className="sm:hidden absolute bottom-0 left-0 right-0 z-[1002] max-h-[75vh] flex flex-col bg-white rounded-t-2xl shadow-[0_-8px_30px_rgba(0,0,0,0.12)] animate-slide-in-up">
            <div className="flex items-center justify-between px-4 pt-3 pb-2 border-b border-border flex-shrink-0">
              <div className="w-10 h-1 rounded-full bg-ink/15 mx-auto absolute left-1/2 -translate-x-1/2 top-2" />
              <span className="text-xs font-semibold text-ink-muted uppercase tracking-wider">
                {panelType === 'iris' ? 'Quartier' : 'Commune'}
              </span>
              <button onClick={closePanel} className="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-border transition-colors text-ink-light hover:text-ink">
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-4">
              {panelType === 'commune' && <CommunePanel codeInsee={panelCode} onClose={closePanel} />}
              {panelType === 'iris' && <IrisPanel codeIris={panelCode} onClose={closePanel} />}
            </div>
          </div>
        </>
      )}

    </div>
  )
}
