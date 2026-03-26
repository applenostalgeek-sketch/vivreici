import { useNavigate, Link } from 'react-router-dom'
import { useState, useEffect } from 'react'
import SearchBar from '../components/SearchBar.jsx'
import Nav from '../components/Nav.jsx'
import { usePageMeta } from '../hooks/usePageMeta.js'
import { SCORE_COLORS } from '../constants.js'
import { loadCommunes } from '../hooks/useSearch.js'

const CATEGORIES = [
  { icon: '🏪', label: 'Équipements',           desc: 'Commerces, services publics, équipements de proximité' },
  { icon: '🔒', label: 'Sécurité',              desc: 'Taux de criminalité 2024' },
  { icon: '🏡', label: 'Accessibilité logement',desc: 'Prix m² vs médiane nationale' },
  { icon: '🏥', label: 'Santé',                 desc: 'Accessibilité aux médecins généralistes' },
  { icon: '🎓', label: 'Éducation',             desc: 'IPS collèges + résultats brevet' },
  { icon: '🚆', label: 'Transports',            desc: 'Gare SNCF + arrêts bus/métro/tram' },
  { icon: '🌿', label: 'Environnement',         desc: 'Part d\'espaces naturels et agricoles' },
  { icon: '📈', label: 'Démographie',           desc: 'Évolution de la population' },
]

export default function Home() {
  const navigate = useNavigate()
  const [stats, setStats] = useState(null)
  const [topCommunes, setTopCommunes] = useState([])

  usePageMeta({
    title: null,
    description: 'Trouvez où il fait bon vivre en France. Score A à E pour 35 000 communes — équipements, sécurité, immobilier, santé, éducation, transports.',
  })

  useEffect(() => {
    fetch('/data/stats.json').then(r => r.json()).then(setStats).catch(() => {})
    loadCommunes().then(communes => {
      const top = communes
        .filter(c => c.score_global != null && (c.population || 0) >= 15000)
        .sort((a, b) => (b.score_global ?? -1) - (a.score_global ?? -1))
        .slice(0, 5)
        .map(c => ({ ...c, score: { lettre: c.lettre, score_global: c.score_global } }))
      setTopCommunes(top)
    }).catch(() => {})
  }, [])

  return (
    <div className="min-h-screen flex flex-col bg-paper">

      {/* ── HERO SOMBRE ────────────────────────────────────────────── */}
      <div className="hero-dark relative overflow-hidden">
        {/* Nav transparente sur fond sombre */}
        <nav className="flex items-center justify-between px-6 py-5 relative z-10">
          <span className="font-display text-xl tracking-tight text-paper">
            <span className="font-light">le</span><span className="font-extrabold text-score-A">bon</span><span className="font-light">quartier</span>
          </span>
          <a href="/carte" className="text-sm font-medium text-paper/50 hover:text-paper/90 transition-colors">
            Carte
          </a>
        </nav>

        <main className="relative z-10 flex flex-col items-center px-6 pt-12 pb-20">

          {/* Badge */}
          <div className="inline-flex items-center gap-2 border border-paper/15 rounded-full px-4 py-1.5 text-xs text-paper/50 mb-10 animate-fade-up">
            <span className="w-1.5 h-1.5 rounded-full bg-score-A inline-block" />
            open data · {stats ? `${stats.nb_scorees.toLocaleString('fr-FR')} communes scorées` : '35 000+ communes'}
          </div>

          {/* H1 */}
          <h1
            className="font-display text-5xl md:text-7xl lg:text-8xl text-paper text-center leading-[1.05] mb-6 animate-fade-up max-w-3xl"
            style={{ animationDelay: '60ms', opacity: 0, animationFillMode: 'forwards' }}
          >
            Tu cherches<br />
            <em className="not-italic text-score-A">où t'installer&nbsp;?</em>
          </h1>

          <p
            className="text-paper/50 text-base md:text-lg text-center max-w-sm mb-12 animate-fade-up"
            style={{ animationDelay: '140ms', opacity: 0, animationFillMode: 'forwards' }}
          >
            On classe les meilleures communes selon 8 critères objectifs.
          </p>

          {/* SearchBar */}
          <div
            className="w-full max-w-lg animate-fade-up"
            style={{ animationDelay: '300ms', opacity: 0, animationFillMode: 'forwards' }}
          >
            <SearchBar size="md" placeholder="Commune ou adresse précise…" />
          </div>

          {/* Score preview strip — communes réelles */}
          {topCommunes.length > 0 && (
            <div
              className="mt-16 w-full max-w-2xl animate-fade-up"
              style={{ animationDelay: '400ms', opacity: 0, animationFillMode: 'forwards' }}
            >
              <p className="text-xs text-paper/25 text-center mb-4 uppercase tracking-widest">Exemples de scores</p>
              <div className="flex flex-wrap justify-center gap-3">
                {topCommunes.map(c => (
                  <button
                    key={c.code_insee}
                    onClick={() => navigate(`/commune/${c.code_insee}`)}
                    className="flex items-center gap-2.5 border border-paper/10 rounded-xl px-4 py-2.5 hover:border-paper/25 hover:bg-paper/5 transition-all group"
                  >
                    <div
                      className="w-7 h-7 rounded-lg flex items-center justify-center font-display font-bold text-white text-sm flex-shrink-0"
                      style={{ backgroundColor: SCORE_COLORS[c.score?.lettre] }}
                    >
                      {c.score?.lettre}
                    </div>
                    <span className="text-sm text-paper/60 group-hover:text-paper/90 transition-colors">
                      {c.nom}
                    </span>
                  </button>
                ))}
              </div>
            </div>
          )}
        </main>
      </div>

      {/* ── STATS ───────────────────────────────────────────────────── */}
      {stats && (
        <section className="bg-white border-b border-border px-6 py-10">
          <div className="max-w-3xl mx-auto grid grid-cols-2 md:grid-cols-4 gap-8 text-center">
            {[
              { value: stats.nb_scorees.toLocaleString('fr-FR'), label: 'communes scorées' },
              { value: stats.nb_iris ? `${Math.round(stats.nb_iris / 1000)} 000+` : '48 000+', label: 'quartiers analysés' },
              { value: '8', label: 'critères objectifs' },
              { value: '100 %', label: 'open data public' },
            ].map(({ value, label }) => (
              <div key={label}>
                <div className="font-display text-3xl text-ink mb-1">{value}</div>
                <div className="text-xs text-ink-light">{label}</div>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* ── CATÉGORIES ──────────────────────────────────────────────── */}
      <section className="px-6 py-16">
        <div className="max-w-3xl mx-auto">
          <h2 className="font-display text-3xl text-ink mb-2 text-center">Ce qu'on mesure</h2>
          <p className="text-ink-light text-sm text-center mb-10">
            Chaque commune comparée aux 35 000 autres — percentile national.
          </p>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {CATEGORIES.map(({ icon, label, desc }) => (
              <div key={label} className="flex items-start gap-3 bg-white border border-border rounded-xl px-4 py-4">
                <span className="text-lg flex-shrink-0 mt-0.5">{icon}</span>
                <div>
                  <div className="font-semibold text-sm text-ink">{label}</div>
                  <div className="text-xs text-ink-light mt-0.5 leading-relaxed">{desc}</div>
                </div>
              </div>
            ))}
          </div>
          <p className="text-center text-xs text-ink-light mt-8">
            Sources : BPE 2024 INSEE · DVF 2024 DGFiP · SSMSI 2024 · APL 2023 DREES · CEREMA 2023 · Filosofi 2021 INSEE ·{' '}
            <Link to="/methode" className="underline hover:text-ink">Méthode complète</Link>
          </p>
        </div>
      </section>

      {/* ── FOOTER ──────────────────────────────────────────────────── */}
      <footer className="border-t border-border px-6 py-5 text-center text-xs text-ink-light">
        lebonquartier · open data français · 2024 ·{' '}
        <Link to="/methode" className="underline hover:text-ink">Méthode</Link>
        {' · '}
        <a href="/carte" className="underline hover:text-ink">Carte</a>
      </footer>

    </div>
  )
}
