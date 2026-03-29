import { createContext, useContext, useState, useCallback } from 'react'
import { PROFILES } from '../utils/profiles.js'

const ProfileContext = createContext(null)

export function ProfileProvider({ children }) {
  const [profile, setProfile] = useState(
    () => {
      const saved = localStorage.getItem('lbq_profile')
      return saved && PROFILES[saved] ? saved : 'national'
    }
  )

  const selectProfile = useCallback((key) => {
    setProfile(key)
    localStorage.setItem('lbq_profile', key)
  }, [])

  return (
    <ProfileContext.Provider value={{ profile, selectProfile }}>
      {children}
    </ProfileContext.Provider>
  )
}

export function useProfile() {
  return useContext(ProfileContext)
}
