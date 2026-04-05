import { Link } from 'react-router-dom'
import Nav from '../components/Nav.jsx'
import MapView from '../components/MapView.jsx'
import { usePageMeta } from '../hooks/usePageMeta.js'

export default function Home() {
  usePageMeta({
    title: null,
    description: 'Trouvez où il fait bon vivre en France. Score A à E pour 35 000 communes — équipements, sécurité, immobilier, santé, éducation, transports.',
  })

  return (
    <div className="h-[100dvh] flex flex-col overflow-hidden relative">

      {/* ── MAP (fond) ───────────────────────────────────────────── */}
      <div className="absolute inset-0 z-0">
        <MapView className="h-full w-full" />
      </div>

      {/* ── HEADER — même Nav que les autres pages, en overlay ──── */}
      <Nav overlay>
        <Link to="/classement" className="text-sm font-medium text-ink-light hover:text-ink transition-colors">
          Classement
        </Link>
      </Nav>

      {/* ── TAGLINE (flottant sur la carte) ──────────────────────── */}
      <div className="absolute z-[1000] bottom-4 sm:bottom-8 left-1/2 -translate-x-1/2 bg-white/90 backdrop-blur-md rounded-full px-4 py-2 shadow-lg border border-border/50">
        <p className="text-xs sm:text-sm font-display font-bold text-ink text-center">
          Qualité de vie des communes françaises <span className="font-normal text-ink-light">· Score A à E</span>
        </p>
      </div>

    </div>
  )
}
