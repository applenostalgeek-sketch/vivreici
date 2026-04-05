import { Link } from 'react-router-dom'
import ProfileDropdown from './ProfileDropdown.jsx'

/**
 * Barre de navigation commune à toutes les pages.
 * Logo + profil uniquement — la recherche est dans la carte.
 */
export default function Nav({ overlay = false, onLogoClick }) {
  return (
    <nav className={`flex items-center justify-between px-4 sm:px-6 py-3 sm:py-4 border-b border-border backdrop-blur-sm ${
      overlay
        ? 'absolute top-0 left-0 right-0 z-[1001] bg-white/95'
        : 'bg-white/60 sticky top-0 z-40'
    }`}>
      <Link to="/" onClick={onLogoClick} className="font-display text-xl tracking-tight text-ink -ml-2 px-2 py-1">
        <span className="font-light">le</span><span className="font-extrabold text-score-A">bon</span><span className="font-light">quartier</span>
      </Link>

      <ProfileDropdown />
    </nav>
  )
}
