import { Routes, Route } from 'react-router-dom'
import Home from './pages/Home.jsx'
import Commune from './pages/Commune.jsx'
import Classement from './pages/Classement.jsx'
import Carte from './pages/Carte.jsx'
import Iris from './pages/Iris.jsx'
import QuartiersCommune from './pages/QuartiersCommune.jsx'
import CompareIris from './pages/CompareIris.jsx'
import Recherche from './pages/Recherche.jsx'
import Methode from './pages/Methode.jsx'
import CompareCommunes from './pages/CompareCommunes.jsx'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/commune/:codeInsee" element={<Commune />} />
      <Route path="/commune/:codeInsee/quartiers" element={<QuartiersCommune />} />
      <Route path="/iris/:codeIris" element={<Iris />} />
      <Route path="/comparer-iris" element={<CompareIris />} />
      <Route path="/classement" element={<Classement />} />
      <Route path="/carte" element={<Carte />} />
      <Route path="/recherche" element={<Recherche />} />
      <Route path="/methode" element={<Methode />} />
      <Route path="/comparer" element={<CompareCommunes />} />
    </Routes>
  )
}
