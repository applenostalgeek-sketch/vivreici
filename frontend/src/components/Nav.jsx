import { useState, useRef, useEffect } from 'react'
import { Link } from 'react-router-dom'
import ProfileDropdown from './ProfileDropdown.jsx'

/**
 * Barre de navigation.
 * Desktop : Logo | À propos · Classement | Profil
 * Mobile  : Logo | Profil | ☰ (À propos + Classement)
 */
export default function Nav({ overlay = false, onLogoClick }) {
  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef(null)

  useEffect(() => {
    function onOutside(e) {
      if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false)
    }
    document.addEventListener('mousedown', onOutside)
    return () => document.removeEventListener('mousedown', onOutside)
  }, [])

  return (
    <nav className={`flex items-center justify-between px-4 sm:px-6 py-3 sm:py-4 border-b border-border backdrop-blur-sm ${
      overlay
        ? 'absolute top-0 left-0 right-0 z-[1001] bg-white/95'
        : 'bg-white/60 sticky top-0 z-40'
    }`}>
      {/* Logo */}
      <Link to="/" onClick={onLogoClick} className="font-display text-xl tracking-tight text-ink -ml-2 px-2 py-1">
        <span className="font-light">le</span><span className="font-extrabold text-score-A">bon</span><span className="font-light">quartier</span>
      </Link>

      {/* Desktop : liens + profil */}
      <div className="hidden sm:flex items-center gap-5">
        <Link to="/classement" className="text-sm text-ink-light hover:text-ink transition-colors">
          Classement
        </Link>
        <Link to="/a-propos" className="text-sm text-ink-light hover:text-ink transition-colors">
          À propos
        </Link>
        <ProfileDropdown />
      </div>

      {/* Mobile : profil + hamburger */}
      <div className="flex sm:hidden items-center gap-2">
        <ProfileDropdown />
        <div ref={menuRef} className="relative">
          <button
            onClick={() => setMenuOpen(o => !o)}
            className="flex items-center justify-center w-9 h-9 rounded-lg border border-border text-ink-light hover:text-ink hover:border-ink/40 transition-colors"
            aria-label="Menu"
          >
            {menuOpen ? (
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
            ) : (
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5" />
              </svg>
            )}
          </button>

          {menuOpen && (
            <div className="absolute right-0 top-full mt-1.5 bg-white border border-border rounded-xl shadow-xl z-50 min-w-[160px] overflow-hidden py-1">
              <Link
                to="/classement"
                onClick={() => setMenuOpen(false)}
                className="flex items-center gap-2.5 px-4 py-2.5 text-sm text-ink-light hover:bg-paper hover:text-ink transition-colors"
              >
                Classement
              </Link>
              <Link
                to="/a-propos"
                onClick={() => setMenuOpen(false)}
                className="flex items-center gap-2.5 px-4 py-2.5 text-sm text-ink-light hover:bg-paper hover:text-ink transition-colors"
              >
                À propos
              </Link>
            </div>
          )}
        </div>
      </div>
    </nav>
  )
}
