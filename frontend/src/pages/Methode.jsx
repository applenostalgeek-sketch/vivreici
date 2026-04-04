import { Link } from 'react-router-dom'
import Nav from '../components/Nav.jsx'

const LETTRES = [
  { l: 'A', range: '80 – 100', interp: 'Top 20 % national',        cls: 'text-score-A', bg: 'bg-score-A/10' },
  { l: 'B', range: '60 – 79',  interp: 'Au-dessus de la médiane',  cls: 'text-score-B', bg: 'bg-score-B/10' },
  { l: 'C', range: '40 – 59',  interp: 'Dans la moyenne',          cls: 'text-score-C', bg: 'bg-score-C/10' },
  { l: 'D', range: '20 – 39',  interp: 'En dessous de la médiane', cls: 'text-score-D', bg: 'bg-score-D/10' },
  { l: 'E', range: '0 – 19',   interp: 'Bas 20 % national',        cls: 'text-score-E', bg: 'bg-score-E/10' },
]

const CATEGORIES = [
  { icon: '🏪', nom: 'Équipements', poids: 20, detail: 'Score hybride présence + densité, pondéré par la taille de la commune. Petites communes (< 20 000 hab) : on mesure la variété des services présents — avoir une pharmacie compte, pas d\'en avoir 50. Grandes communes (> 20 000 hab) : on mesure la densité pondérée d\'équipements pour 10 000 habitants. Transition douce entre les deux régimes. 13 types comptent dans le score : pharmacie, supermarché, hypermarché (poids fort), boulangerie, poste, hôpital, urgences (poids moyen), boucherie, gymnase, piscine, cinéma, bibliothèque, théâtre (poids faible). Médecins, écoles et gares sont exclus car déjà couverts par leurs catégories respectives.', src: 'BPE 2024 INSEE' },
  { icon: '🚆', nom: 'Transports', poids: 18, detail: 'Composite 50 % distance à la gare SNCF la plus proche (inversé — plus proche = mieux) + 50 % densité d\'arrêts de transport en commun (bus, métro, tram, RER) dans la commune. Les deux composantes sont converties en percentile national avant d\'être combinées.', src: 'SNCF · transport.data.gouv.fr' },
  { icon: '🏥', nom: 'Santé', poids: 18, detail: 'Accessibilité Potentielle Localisée (APL) — nombre de consultations accessibles par habitant et par an chez un médecin généraliste. Méthode DREES : modèle gravitaire qui tient compte de l\'offre ET de la demande dans un rayon autour de chaque commune, pondéré par la distance. Ce n\'est pas un simple comptage de médecins : une commune proche d\'une grande ville bien dotée bénéficie de son offre.', src: 'APL 2023 DREES' },
  { icon: '🔒', nom: 'Sécurité', poids: 14, detail: 'Taux de criminalité pour 1 000 habitants — score inversé (moins de faits = meilleur score). Somme de 6 catégories : cambriolages, violences physiques (intra et hors famille), vols sans violence, vols violents sans arme, vols avec armes. Communes sous secret statistique (≤ 5 faits sur 3 ans) : le SSMSI fournit la moyenne départementale des communes non diffusées, utilisée comme estimation. Percentile national inversé.', src: 'SSMSI 2025' },
  { icon: '🏡', nom: 'Immobilier', poids: 14, detail: 'Prix au m² médian des transactions réelles (appartements + maisons) — score inversé (moins cher = meilleur score). Seules les ventes avec un prix entre 200 et 20 000 €/m² et une surface >= 10 m² sont retenues. Minimum 5 transactions requises pour que la médiane soit fiable. Alsace-Moselle (57, 67, 68) : pas de DVF (livre foncier local), données issues de l\'API notaires (immobilier.notaires.fr). Autres communes sans données : estimation par les communes les plus proches dans la même tranche de population, pondérées par 1/distance². Quartiers IRIS : les transactions sont géolocalisées par GPS dans chaque quartier.', src: 'DVF 2024 DGFiP · Notaires (Alsace-Moselle)' },
  { icon: '⚠️', nom: 'Risques naturels', poids: 9, detail: 'Plans de Prévention des Risques (PPR) approuvés — score inversé (moins de risques = mieux). Composite : inondation (35 %), séisme (30 %), mouvement de terrain (20 %), feux de forêt (10 %), avalanche (5 %). L\'absence de PPR ne signifie pas l\'absence de risque.', src: 'GASPAR Géorisques' },
  { icon: '🎓', nom: 'Éducation', poids: 8, detail: 'Qualité (90 %) : composite IPS collèges (indice de position sociale, 40 %) + résultats au DNB brevet (40 %) + lycées professionnels (20 %), agrégés sur tous les établissements dans un rayon de 30 km pondérés par 1/distance. Proximité (10 %) : distance au collège le plus proche. Cette méthode couvre 100 % des communes, même celles sans établissement.', src: 'DEPP 2021-2025' },
  { icon: '🌿', nom: 'Environnement', poids: 8, detail: 'Composite 50 % artificialisation des sols (taux d\'espaces naturels non artificialisés, CEREMA) + 50 % qualité de l\'air (indice ATMO moyen annuel sur 12 mois glissants). Communes sans données qualité de l\'air (37 %, surtout rurales) : score basé sur l\'artificialisation seule.', src: 'CEREMA 2023 · Atmo France' },
  { icon: '📈', nom: 'Démographie', poids: 4, detail: 'Évolution de la population sur 5 ans (2016 → 2021). Une commune en croissance démographique obtient un meilleur score. Percentile national direct.', src: 'Populations légales INSEE' },
]

const MAX_POIDS = 20

const PROFILES = [
  {
    emoji: '👪', label: 'Famille',
    desc: 'Priorité à l\'école, à la sécurité et aux services du quotidien.',
    top: [
      { nom: 'Éducation',  poids: 27, icon: '🎓' },
      { nom: 'Sécurité',   poids: 22, icon: '🔒' },
      { nom: 'Équipements',poids: 18, icon: '🏪' },
    ],
  },
  {
    emoji: '⚡', label: 'Jeune actif',
    desc: 'Se déplacer vite, se loger à prix raisonnable, profiter de la ville.',
    top: [
      { nom: 'Transports',  poids: 30, icon: '🚆' },
      { nom: 'Immobilier',  poids: 26, icon: '🏡' },
      { nom: 'Équipements', poids: 18, icon: '🏪' },
    ],
  },
  {
    emoji: '🌿', label: 'Calme',
    desc: 'Nature, tranquillité, coût de la vie maîtrisé.',
    top: [
      { nom: 'Environnement', poids: 28, icon: '🌿' },
      { nom: 'Immobilier',    poids: 20, icon: '🏡' },
      { nom: 'Sécurité',      poids: 18, icon: '🔒' },
    ],
  },
  {
    emoji: '☀️', label: 'Retraite',
    desc: 'Accès aux soins, sécurité et cadre de vie agréable.',
    top: [
      { nom: 'Santé',         poids: 32, icon: '🏥' },
      { nom: 'Sécurité',      poids: 22, icon: '🔒' },
      { nom: 'Environnement', poids: 16, icon: '🌿' },
    ],
  },
  {
    emoji: '💰', label: 'Budget',
    desc: 'Le prix au m² avant tout, sans sacrifier la mobilité.',
    top: [
      { nom: 'Immobilier',  poids: 40, icon: '🏡' },
      { nom: 'Transports',  poids: 16, icon: '🚆' },
      { nom: 'Équipements', poids: 15, icon: '🏪' },
    ],
  },
]

const LIMITES = [
  { titre: 'Le cadre de vie', txt: 'L\'ambiance du quartier, les projets d\'urbanisme ou vos préférences personnelles ne sont pas mesurables. Une commune C peut être le meilleur choix pour vous.' },
  { titre: 'Les revenus', txt: 'Volontairement exclus du score — affichés en information uniquement, pour ne pas avantager les communes aisées au détriment des autres.' },
  { titre: 'Petites communes', txt: 'Les communes rurales ont souvent moins de données disponibles. Le score repose alors sur 3 à 5 catégories au lieu de 9.' },
  { titre: 'Risques naturels', txt: 'Basés sur les PPR approuvés (GASPAR). L\'absence de PPR ne signifie pas l\'absence de risque — certaines communes exposées n\'ont pas encore fait l\'objet d\'une procédure réglementaire.' },
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

        {/* Le score A–E */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-1">Le score</h2>
          <p className="text-sm text-ink-light leading-relaxed mb-6">
            Chaque commune est comparée à l'ensemble des communes françaises par percentile.
            Un score de <strong className="text-ink">80</strong> signifie que la commune fait mieux que 80 % des communes.
            La médiane nationale est à <strong className="text-ink">50</strong> par construction.
            Le score global est une moyenne pondérée des 9 catégories ci-dessous.
          </p>
          <div className="space-y-2">
            {LETTRES.map(({ l, range, interp, cls, bg }) => (
              <div key={l} className={`flex items-center gap-4 rounded-xl px-4 py-2.5 ${bg}`}>
                <span className={`font-display font-bold text-2xl w-6 text-center ${cls}`}>{l}</span>
                <span className="font-mono text-sm text-ink w-20">{range}</span>
                <span className="text-sm text-ink-light">{interp}</span>
              </div>
            ))}
          </div>
        </section>

        {/* Catégories */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-1">Les 9 catégories</h2>
          <p className="text-xs text-ink-light mb-6">Poids dans le score national entre parenthèses.</p>
          <div className="space-y-5">
            {CATEGORIES.map(({ icon, nom, poids, detail, src }) => (
              <div key={nom} className="flex gap-3">
                <span className="text-base flex-shrink-0 w-6 mt-0.5">{icon}</span>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-sm font-medium text-ink">{nom}</span>
                    <span className="text-xs text-ink-light">{poids} %</span>
                    <div className="flex-1 h-1 bg-border rounded-full overflow-hidden">
                      <div
                        className="h-full bg-ink/20 rounded-full"
                        style={{ width: `${(poids / MAX_POIDS) * 100}%` }}
                      />
                    </div>
                  </div>
                  <p className="text-xs text-ink-light leading-relaxed">{detail}</p>
                  <p className="text-xs font-mono text-ink-light/40 mt-0.5">{src}</p>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Profils */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-1">Les profils</h2>
          <p className="text-sm text-ink-light leading-relaxed mb-6">
            Les profils recalculent le score en réattribuant les poids selon vos priorités.
            Le score national reste toujours affiché en référence — le profil s'y ajoute,
            jamais à sa place. Sélectionnable depuis la carte ou la recherche.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {PROFILES.map(({ emoji, label, desc, top }) => (
              <div key={label} className="border border-border rounded-xl p-4">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-lg">{emoji}</span>
                  <span className="font-medium text-ink text-sm">{label}</span>
                </div>
                <p className="text-xs text-ink-light mb-3 leading-relaxed">{desc}</p>
                <div className="space-y-1.5">
                  {top.map(({ nom, poids, icon }, i) => (
                    <div key={nom} className="flex items-center gap-2">
                      <span className="text-xs text-ink-muted w-3">{i + 1}</span>
                      <span className="text-xs">{icon}</span>
                      <span className="text-xs text-ink flex-1">{nom}</span>
                      <span className="text-xs font-mono text-ink-light">{poids} %</span>
                      <div className="w-12 h-1 bg-border rounded-full overflow-hidden">
                        <div
                          className="h-full bg-ink/30 rounded-full"
                          style={{ width: `${(poids / 40) * 100}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Quartiers */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-3">Les quartiers (IRIS)</h2>
          <p className="text-sm text-ink-light leading-relaxed">
            En zoomant sur la carte, chaque commune est découpée en quartiers IRIS —
            les zones géographiques de référence de l'INSEE (~2 000 habitants chacune).
            48 569 quartiers sont disponibles sur toute la France.
          </p>
          <p className="text-sm text-ink-light leading-relaxed mt-3">
            Les quartiers sont scorés sur les <strong className="text-ink">9 mêmes catégories</strong> que les communes.
            Trois catégories sont mesurées localement à l'échelle du quartier :
            équipements (BPE par IRIS), santé (médecins par IRIS) et immobilier (transactions DVF géolocalisées par GPS dans chaque quartier).
            Les six autres (sécurité, transports, éducation, environnement, démographie, risques naturels)
            reprennent le score de la commune parente — ces données ne sont pas disponibles à l'échelle du quartier.
            Si un quartier manque de données locales, il hérite du score de sa commune.
            Mêmes poids que les communes — scores comparables.
          </p>
        </section>

        {/* Limites */}
        <section className="bg-white border border-border rounded-2xl p-6 mb-4">
          <h2 className="font-display text-xl text-ink mb-4">Ce que le score ne mesure pas</h2>
          <div className="space-y-4">
            {LIMITES.map(({ titre, txt }) => (
              <div key={titre} className="flex gap-3">
                <span className="flex-shrink-0 font-mono text-ink mt-0.5">—</span>
                <div>
                  <span className="text-sm font-medium text-ink">{titre} </span>
                  <span className="text-sm text-ink-light leading-relaxed">{txt}</span>
                </div>
              </div>
            ))}
          </div>
        </section>

        <p className="text-center text-xs text-ink-light pt-2">
          Toutes les données utilisées sont des open data françaises, publiquement vérifiables.
        </p>

      </main>

      <footer className="border-t border-border px-6 py-5 text-center text-xs text-ink-light">
        lebonquartier · open data français · 2026 ·{' '}
        <Link to="/methode" className="underline hover:text-ink">Méthode</Link>
      </footer>
    </div>
  )
}
