import { useEffect } from 'react'

export function usePageMeta({ title, description }) {
  useEffect(() => {
    // Title
    document.title = title ? `${title} | lebonquartier` : 'lebonquartier — Le score de qualité de vie des communes françaises'

    // Meta description
    let metaDesc = document.querySelector('meta[name="description"]')
    if (!metaDesc) {
      metaDesc = document.createElement('meta')
      metaDesc.name = 'description'
      document.head.appendChild(metaDesc)
    }
    metaDesc.content = description || 'Découvrez le score de qualité de vie de votre commune — équipements, sécurité, santé, éducation, transports. Données open data INSEE 2024.'

    // OG tags
    const setMeta = (property, content) => {
      let el = document.querySelector(`meta[property="${property}"]`)
      if (!el) {
        el = document.createElement('meta')
        el.setAttribute('property', property)
        document.head.appendChild(el)
      }
      el.content = content
    }
    setMeta('og:title', document.title)
    setMeta('og:description', metaDesc.content)
    setMeta('og:type', 'website')
  }, [title, description])
}
