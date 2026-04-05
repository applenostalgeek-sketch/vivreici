import { useState, useRef, useEffect } from 'react'
import { useProfile } from '../context/ProfileContext.jsx'
import { PROFILES } from '../utils/profiles.js'

/**
 * Dropdown de sélection du profil utilisateur.
 * dark=true : version fond sombre (hero homepage)
 * dark=false (défaut) : version fond clair (Nav standard)
 */
export default function ProfileDropdown({ dark = false }) {
  const { profile, selectProfile } = useProfile()
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  const p = PROFILES[profile]
  const isCustom = profile !== 'national'

  useEffect(() => {
    function onOutside(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onOutside)
    return () => document.removeEventListener('mousedown', onOutside)
  }, [])

  const labelFull = isCustom ? `${p.emoji} ${p.label}` : 'Score national'
  const labelShort = isCustom ? p.emoji : '🇫🇷'

  const btnClass = dark
    ? isCustom
      ? 'border-paper/50 text-paper bg-paper/15 hover:bg-paper/25'
      : 'border-paper/25 text-paper/60 hover:border-paper/40 hover:text-paper/90'
    : isCustom
      ? 'bg-ink text-paper border-ink hover:bg-ink/80'
      : 'border-border text-ink-light hover:border-ink/40 hover:text-ink'

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(o => !o)}
        className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium border transition-all ${btnClass}`}
      >
        <span className="hidden sm:inline">{labelFull}</span>
        <span className="sm:hidden">{labelShort}</span>
        <svg
          className={`w-3 h-3 transition-transform ${open ? 'rotate-180' : ''} ${dark ? 'opacity-40' : 'opacity-50'}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="m19 9-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1.5 bg-white border border-border rounded-xl shadow-xl z-50 min-w-[190px] overflow-hidden py-1">
          {Object.entries(PROFILES).map(([key, prof]) => (
            <button
              key={key}
              onClick={() => { selectProfile(key); setOpen(false) }}
              className={`w-full flex items-center gap-2.5 px-4 py-2.5 text-sm text-left transition-colors ${
                profile === key
                  ? 'bg-paper text-ink font-medium'
                  : 'text-ink-light hover:bg-paper hover:text-ink'
              }`}
            >
              <span className="w-4 text-center text-base leading-none">{prof.emoji ?? '·'}</span>
              <span>{prof.label}</span>
              {profile === key && (
                <span className="ml-auto text-score-A text-xs">✓</span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
