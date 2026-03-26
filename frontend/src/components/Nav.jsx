import { Link } from 'react-router-dom'
import SearchBar from './SearchBar.jsx'

/**
 * Barre de navigation commune à toutes les pages.
 *
 * Props :
 * - searchBar       : afficher la SearchBar (défaut: true)
 * - searchPlaceholder: texte placeholder de la SearchBar
 * - children        : liens custom à droite — si omis, affiche les liens standards
 */
export default function Nav({ searchBar = true, searchPlaceholder = 'Commune ou adresse…', children }) {
  return (
    <nav className="flex items-center justify-between px-6 py-4 border-b border-border bg-white/60 backdrop-blur-sm sticky top-0 z-40">
      <Link to="/" className="font-display text-xl tracking-tight text-ink">
        <span className="font-light">le</span><span className="font-extrabold text-score-A">bon</span><span className="font-light">quartier</span>
      </Link>

      {searchBar && (
        <div className="w-64 hidden md:block">
          <SearchBar size="sm" placeholder={searchPlaceholder} />
        </div>
      )}

      <div className="flex items-center gap-4">
        {children ?? (
          <a href="/carte" className="text-sm font-medium text-ink-light hover:text-ink transition-colors">Carte</a>
        )}
      </div>
    </nav>
  )
}
