import { createI18n } from 'vue-i18n'

// Import locale messages
import en from './locales/en.json'
import ko from './locales/ko.json'
import de from './locales/de.json'
import es from './locales/es.json'
import zh from './locales/zh.json'
import ja from './locales/ja.json'

// Language definitions with metadata
export const languages = [
  { code: 'en', name: 'English', flag: '🇺🇸' },
  { code: 'ko', name: '한국어', flag: '🇰🇷' },
  { code: 'de', name: 'Deutsch', flag: '🇩🇪' },
  { code: 'es', name: 'Español', flag: '🇪🇸' },
  { code: 'zh', name: '中文', flag: '🇨🇳' },
  { code: 'ja', name: '日本語', flag: '🇯🇵' }
]

// Get saved locale or detect from browser
function getDefaultLocale() {
  // Check localStorage first
  const savedLocale = localStorage.getItem('locale')
  if (savedLocale && languages.some(l => l.code === savedLocale)) {
    return savedLocale
  }
  
  // Detect from browser language
  const browserLang = navigator.language.split('-')[0]
  if (languages.some(l => l.code === browserLang)) {
    return browserLang
  }
  
  // Default to English
  return 'en'
}

// Create i18n instance
const i18n = createI18n({
  legacy: false, // Use Composition API mode
  locale: getDefaultLocale(),
  fallbackLocale: 'en',
  messages: {
    en,
    ko,
    de,
    es,
    zh,
    ja
  },
  // Enable HTML in messages (for links, etc.)
  warnHtmlMessage: false
})

// Function to change locale
export function setLocale(locale) {
  if (languages.some(l => l.code === locale)) {
    i18n.global.locale.value = locale
    localStorage.setItem('locale', locale)
    document.documentElement.setAttribute('lang', locale)
  }
}

// Function to get current locale
export function getLocale() {
  return i18n.global.locale.value
}

export default i18n



