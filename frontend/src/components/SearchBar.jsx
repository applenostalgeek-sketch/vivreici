import { useRef, useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { useSearch, locateByCoords } from '../hooks/useSearch.js'

export default function SearchBar({ size = 'lg', placeholder = 'Rechercher une commune ou une adresse…', onSelect }) {
  const { query, setQuery, results, loading } = useSearch()
  const [open, setOpen] = useState(false)
  const [focused, setFocused] = useState(false)
  const [locating, setLocating] = useState(false)
  const navigate = useNavigate()
  const containerRef = useRef(null)
  const inputRef = useRef(null)

  useEffect(() => {
    function onClickOutside(e) {
      if (containerRef.current && !containerRef.current.contains(e.target)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', onClickOutside)
    return () => document.removeEventListener('mousedown', onClickOutside)
  }, [])

  useEffect(() => {
    setOpen(results.length > 0 && focused)
  }, [results, focused])

  async function selectAdresse(adresse) {
    setQuery('')
    setOpen(false)
    setLocating(true)
    inputRef.current?.blur()
    try {
      const loc = await locateByCoords(adresse.lat, adresse.lng)
      const code = loc.code_commune || adresse.code_insee
      if (code) {
        navigate(`/commune/${code}?tab=carte&lat=${adresse.lat}&lng=${adresse.lng}`)
      }
    } catch {
      if (adresse.code_insee) navigate(`/commune/${adresse.code_insee}?tab=carte&lat=${adresse.lat}&lng=${adresse.lng}`)
    } finally {
      setLocating(false)
    }
  }

  function selectCommune(commune) {
    setQuery('')
    setOpen(false)
    inputRef.current?.blur()
    if (onSelect) {
      onSelect(commune)
    } else {
      navigate(`/commune/${commune.code_insee}`)
    }
  }

  function select(item) {
    if (item._type === 'adresse') {
      selectAdresse(item)
    } else {
      selectCommune(item)
    }
  }

  const isLg = size === 'lg'
  const isSpinning = loading || locating

  return (
    <div ref={containerRef} className="relative w-full max-w-2xl">
      <div className={`
        flex items-center gap-3 bg-white border-2 rounded-2xl transition-all duration-200
        ${focused ? 'border-ink shadow-[0_0_0_4px_rgba(28,25,23,0.08)]' : 'border-border shadow-sm'}
        ${isLg ? 'px-5 py-4' : 'px-4 py-3'}
      `}>
        <svg className={`text-ink-light flex-shrink-0 ${isLg ? 'w-5 h-5' : 'w-4 h-4'}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607z" />
        </svg>

        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          onFocus={() => setFocused(true)}
          onBlur={() => setTimeout(() => setFocused(false), 150)}
          placeholder={placeholder}
          className={`
            flex-1 bg-transparent outline-none font-sans text-ink placeholder:text-ink-light/60
            ${isLg ? 'text-lg' : 'text-base'}
          `}
        />

        {isSpinning && (
          <div className="w-4 h-4 border-2 border-border border-t-ink rounded-full animate-spin flex-shrink-0" />
        )}
      </div>

      {open && (
        <div className="absolute top-full left-0 right-0 mt-2 bg-white border border-border rounded-2xl shadow-xl z-50 overflow-hidden animate-scale-in">
          {results.map((item, i) => (
            <button
              key={item._type === 'adresse' ? `addr-${i}` : item.code_insee}
              onClick={() => select(item)}
              className={`
                w-full flex items-center justify-between gap-4 px-5 py-3.5 text-left
                hover:bg-paper transition-colors duration-100
                ${i > 0 ? 'border-t border-border' : ''}
              `}
            >
              {item._type === 'adresse' ? (
                <>
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="text-ink-light flex-shrink-0 text-base">📍</span>
                    <span className="font-medium text-ink truncate">{item.label}</span>
                  </div>
                  <span className="text-xs text-ink-light flex-shrink-0">quartier</span>
                </>
              ) : (
                <>
                  <div className="min-w-0">
                    <span className="font-semibold text-ink">{item.nom}</span>
                    <span className="ml-2 text-sm text-ink-light">
                      {item.codes_postaux?.[0] || item.departement}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-right flex-shrink-0">
                    {item.population > 0 && (
                      <span className="text-xs text-ink-light font-mono">
                        {item.population.toLocaleString('fr-FR')} hab.
                      </span>
                    )}
                    {item.score?.lettre && (
                      <span className={`score-badge score-badge-${item.score.lettre} w-7 h-7 text-sm`}>
                        {item.score.lettre}
                      </span>
                    )}
                  </div>
                </>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
