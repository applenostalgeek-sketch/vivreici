import { Link } from 'react-router-dom'
import SearchBar from '../components/SearchBar.jsx'
import MapView from '../components/MapView.jsx'

export default function Carte() {
  return (
    <div className="h-screen flex flex-col overflow-hidden">
      <nav className="absolute top-0 left-0 right-0 z-[1000] flex items-center gap-3 px-4 py-3 bg-white/95 backdrop-blur-sm border-b border-border">
        <Link to="/" className="font-display text-lg tracking-tight text-ink flex-shrink-0">
          <span className="font-light">le</span><span className="font-extrabold text-score-A">bon</span><span className="font-light">quartier</span>
        </Link>
        <div className="flex-1 max-w-sm">
          <SearchBar size="sm" placeholder="Commune ou adresse…" />
        </div>
        <Link to="/classement" className="hidden lg:flex-shrink-0 text-sm text-ink-light hover:text-ink transition-colors">
          Classement →
        </Link>
      </nav>

      <div className="flex-1 mt-[57px]">
        <MapView className="h-full" />
      </div>
    </div>
  )
}
