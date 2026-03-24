import Nav from '../components/Nav.jsx'

export default function Methode() {
  return (
    <div className="min-h-screen bg-paper">
      <Nav searchPlaceholder="Commune…" />

      <main className="max-w-2xl mx-auto px-6 py-12">
        <h1 className="font-display text-4xl text-ink mb-2">Comment ça marche ?</h1>
        <p className="text-ink-light mb-10">
          Ce que signifie le score, ce qu'il mesure, et ses limites.
        </p>

        {/* Le score */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-4">Le score</h2>
          <p className="text-sm text-ink-light leading-relaxed mb-6">
            Chaque commune est comparée à l'ensemble des communes françaises.
            Un score de <strong className="text-ink">80</strong> signifie que la commune fait mieux que 80 % des communes françaises.
            La médiane nationale est à <strong className="text-ink">50</strong> par construction.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm border-collapse">
              <tbody className="divide-y divide-border">
                {[
                  { l: 'A', range: '80 – 100', interp: 'Top 20 % national', cls: 'text-score-A' },
                  { l: 'B', range: '60 – 79',  interp: 'Au-dessus de la médiane', cls: 'text-score-B' },
                  { l: 'C', range: '40 – 59',  interp: 'Dans la moyenne nationale', cls: 'text-score-C' },
                  { l: 'D', range: '20 – 39',  interp: 'En dessous de la médiane', cls: 'text-score-D' },
                  { l: 'E', range: '0 – 19',   interp: 'Bas 20 % national', cls: 'text-score-E' },
                ].map(({ l, range, interp, cls }) => (
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
          </div>
        </section>

        {/* Ce qui est mesuré */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-4">Ce qui est mesuré</h2>
          <div className="space-y-3">
            {[
              { icon: '🏪', nom: 'Équipements', detail: 'Commerces, services, équipements de proximité', src: 'BPE 2024 INSEE' },
              { icon: '🚆', nom: 'Transports',  detail: 'Distance gare SNCF + densité arrêts bus/métro/tram', src: 'SNCF + transport.data.gouv.fr' },
              { icon: '🏥', nom: 'Santé',       detail: 'Accessibilité aux médecins généralistes (APL)', src: 'APL 2023 DREES' },
              { icon: '🔒', nom: 'Sécurité',    detail: 'Taux de criminalité (score inversé)', src: 'SSMSI 2024' },
              { icon: '🏡', nom: 'Prix au m²',   detail: 'Score élevé = marché abordable · Score bas = marché cher', src: 'DVF 2024 DGFiP' },
              { icon: '🎓', nom: 'Éducation',   detail: 'IPS collèges + résultats brevet + lycées pro', src: 'DEPP 2021-2025' },
              { icon: '🌿', nom: 'Environnement', detail: 'Part d\'espaces naturels et agricoles', src: 'CEREMA 2023' },
              { icon: '📈', nom: 'Démographie', detail: 'Évolution de la population sur 5 ans', src: 'INSEE 2016-2021' },
            ].map(({ icon, nom, detail, src }) => (
              <div key={nom} className="flex items-center gap-3">
                <span className="text-lg flex-shrink-0 w-7">{icon}</span>
                <div className="flex-1 min-w-0">
                  <span className="text-sm font-medium text-ink">{nom}</span>
                  <span className="text-sm text-ink-light"> — {detail}</span>
                </div>
                <span className="text-xs font-mono text-ink-light/60 flex-shrink-0 hidden sm:block">{src}</span>
              </div>
            ))}
          </div>
        </section>

        {/* Limites */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-4">Ce que le score ne dit pas</h2>
          <ul className="space-y-2 text-sm text-ink-light">
            <li className="flex gap-3">
              <span className="flex-shrink-0 font-mono text-ink">—</span>
              <span>Le cadre de vie, l'ambiance, les projets d'urbanisme, ni vos préférences personnelles. Une commune C peut être votre meilleur choix.</span>
            </li>
            <li className="flex gap-3">
              <span className="flex-shrink-0 font-mono text-ink">—</span>
              <span>Les revenus ne sont pas dans le score — affichés en info uniquement, pour ne pas favoriser les communes riches.</span>
            </li>
            <li className="flex gap-3">
              <span className="flex-shrink-0 font-mono text-ink">—</span>
              <span>Les petites communes rurales ont souvent moins de données, le score repose alors sur 3 à 5 catégories.</span>
            </li>
          </ul>
        </section>

        <p className="text-center text-xs text-ink-light pt-2">
          Toutes les données sont open data françaises, publiquement vérifiables.
        </p>
      </main>
    </div>
  )
}
