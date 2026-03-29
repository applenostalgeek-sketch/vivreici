import { Link } from 'react-router-dom'
import Nav from '../components/Nav.jsx'

const LETTRES = [
  { l: 'A', range: '80 – 100', interp: 'Top 20 % national',        cls: 'text-score-A' },
  { l: 'B', range: '60 – 79',  interp: 'Au-dessus de la médiane',  cls: 'text-score-B' },
  { l: 'C', range: '40 – 59',  interp: 'Dans la moyenne',          cls: 'text-score-C' },
  { l: 'D', range: '20 – 39',  interp: 'En dessous de la médiane', cls: 'text-score-D' },
  { l: 'E', range: '0 – 19',   interp: 'Bas 20 % national',        cls: 'text-score-E' },
]

const CATEGORIES = [
  { icon: '🏪', nom: 'Équipements',   poids: 20, detail: 'Commerces, services et équipements de proximité',          src: 'BPE 2024 INSEE' },
  { icon: '🚆', nom: 'Transports',    poids: 18, detail: 'Distance gare SNCF + densité arrêts bus, métro, tram',     src: 'SNCF · transport.data.gouv.fr' },
  { icon: '🏥', nom: 'Santé',         poids: 18, detail: 'Accessibilité médecins généralistes (APL — aire de chalandise)', src: 'APL 2023 DREES' },
  { icon: '🔒', nom: 'Sécurité',      poids: 14, detail: 'Taux de criminalité (score inversé — moins = mieux)',      src: 'SSMSI 2024' },
  { icon: '🏡', nom: 'Immobilier',    poids: 14, detail: 'Prix au m² médian (score inversé — moins cher = mieux)',   src: 'DVF 2024 DGFiP' },
  { icon: '🎓', nom: 'Éducation',     poids:  8, detail: 'IPS collèges (40 %) + résultats brevet DNB (40 %) + lycées pro (20 %)', src: 'DEPP 2021-2025' },
  { icon: '🌿', nom: 'Environnement', poids:  8, detail: 'Part d\'espaces naturels et agricoles non artificialisés', src: 'CEREMA 2023' },
  { icon: '📈', nom: 'Démographie',   poids:  4, detail: 'Évolution de la population sur 6 ans (2017 → 2023)',       src: 'Populations de référence INSEE 2023' },
]

const LIMITES = [
  'Le cadre de vie, l\'ambiance du quartier, les projets d\'urbanisme ou vos préférences personnelles. Une commune C peut être le meilleur choix pour vous.',
  'Les revenus ne sont pas dans le score — affichés en information uniquement, pour ne pas avantager les communes aisées au détriment des autres.',
  'Les petites communes rurales ont souvent moins de données disponibles. Le score repose alors sur 3 à 5 catégories au lieu de 8.',
]

export default function Methode() {
  return (
    <div className="min-h-screen bg-paper">
      <Nav searchPlaceholder="Commune…" />

      <main className="max-w-2xl mx-auto px-6 py-12">

        <h1 className="font-display text-4xl text-ink mb-2">Méthode</h1>
        <p className="text-ink-light mb-10">
          Comment le score est calculé, ce qu'il mesure, et ses limites.
        </p>

        {/* Le score */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-3">Le score</h2>
          <p className="text-sm text-ink-light leading-relaxed mb-6">
            Chaque commune est comparée à l'ensemble des communes françaises par percentile.
            Un score de <strong className="text-ink">80</strong> signifie que la commune fait mieux que 80 % des communes.
            La médiane nationale est à <strong className="text-ink">50</strong> par construction.
            Le score global est une moyenne pondérée des 8 catégories ci-dessous.
          </p>
          <table className="w-full text-sm border-collapse">
            <tbody className="divide-y divide-border">
              {LETTRES.map(({ l, range, interp, cls }) => (
                <tr key={l}>
                  <td className="py-2.5 pr-4 w-8">
                    <span className={`font-display font-bold text-lg ${cls}`}>{l}</span>
                  </td>
                  <td className="py-2.5 pr-6 font-mono text-ink text-sm w-24">{range}</td>
                  <td className="py-2.5 text-ink-light text-sm">{interp}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        {/* Catégories */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-1">Les 8 catégories</h2>
          <p className="text-xs text-ink-light mb-5">Poids dans le score global entre parenthèses.</p>
          <div className="space-y-4">
            {CATEGORIES.map(({ icon, nom, poids, detail, src }) => (
              <div key={nom} className="flex gap-3">
                <span className="text-base flex-shrink-0 w-6 mt-0.5">{icon}</span>
                <div className="flex-1 min-w-0">
                  <div className="flex items-baseline gap-2 mb-0.5">
                    <span className="text-sm font-medium text-ink">{nom}</span>
                    <span className="text-xs text-ink-light">({poids} %)</span>
                  </div>
                  <p className="text-xs text-ink-light leading-relaxed">{detail}</p>
                  <p className="text-xs font-mono text-ink-light/50 mt-0.5">{src}</p>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Quartiers */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-3">Les quartiers (IRIS)</h2>
          <p className="text-sm text-ink-light leading-relaxed">
            En zoomant sur la carte, chaque commune est découpée en quartiers IRIS — les zones géographiques de référence de l'INSEE (~2 000 habitants chacune).
            48 569 quartiers sont disponibles sur toute la France.
          </p>
          <p className="text-sm text-ink-light leading-relaxed mt-3">
            Les quartiers sont scorés sur <strong className="text-ink">6 catégories</strong> : équipements, santé et immobilier sont mesurés localement au niveau du quartier.
            Sécurité, transports et éducation reprennent le score de la commune parente (ces données ne sont pas disponibles à l'échelle du quartier).
          </p>
        </section>

        {/* Limites */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-4">Ce que le score ne mesure pas</h2>
          <ul className="space-y-3">
            {LIMITES.map((txt, i) => (
              <li key={i} className="flex gap-3 text-sm text-ink-light">
                <span className="flex-shrink-0 font-mono text-ink">—</span>
                <span className="leading-relaxed">{txt}</span>
              </li>
            ))}
          </ul>
        </section>

        <p className="text-center text-xs text-ink-light pt-2">
          Toutes les données utilisées sont des open data françaises, publiquement vérifiables.
        </p>

      </main>
      <footer className="border-t border-border px-6 py-5 text-center text-xs text-ink-light">
        lebonquartier · open data français · 2026 ·{' '}
        <Link to="/methode" className="underline hover:text-ink">Méthode</Link>
        {' · '}
        <a href="/carte" className="underline hover:text-ink">Carte</a>
      </footer>
    </div>
  )
}
