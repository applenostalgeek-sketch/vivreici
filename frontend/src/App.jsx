import { Routes, Route, Navigate } from 'react-router-dom'
import Home from './pages/Home.jsx'
import Classement from './pages/Classement.jsx'
import QuartiersCommune from './pages/QuartiersCommune.jsx'
import Methode from './pages/Methode.jsx'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/carte" element={<Navigate to="/" replace />} />
      <Route path="/commune/:codeInsee" element={<Home />} />
      <Route path="/iris/:codeIris" element={<Home />} />
      <Route path="/commune/:codeInsee/quartiers" element={<QuartiersCommune />} />
      <Route path="/classement" element={<Classement />} />
      <Route path="/methode" element={<Navigate to="/a-propos" replace />} />
      <Route path="/a-propos" element={<Methode />} />
    </Routes>
  )
}
