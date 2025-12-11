<script setup>
import { ref, computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { languages, setLocale } from '../i18n'

const { locale } = useI18n()
const isOpen = ref(false)

const currentLanguage = computed(() => {
  return languages.find(l => l.code === locale.value) || languages[0]
})

const changeLanguage = (code) => {
  setLocale(code)
  isOpen.value = false
}

// Close dropdown when clicking outside
const closeDropdown = (e) => {
  if (!e.target.closest('.language-selector')) {
    isOpen.value = false
  }
}

// Add/remove click listener
import { onMounted, onUnmounted } from 'vue'
onMounted(() => document.addEventListener('click', closeDropdown))
onUnmounted(() => document.removeEventListener('click', closeDropdown))
</script>

<template>
  <div class="language-selector">
    <button class="selector-button" @click="isOpen = !isOpen">
      <span class="flag">{{ currentLanguage.flag }}</span>
      <span class="lang-code">{{ currentLanguage.code.toUpperCase() }}</span>
      <svg 
        class="chevron" 
        :class="{ open: isOpen }"
        width="12" 
        height="12" 
        viewBox="0 0 24 24" 
        fill="none" 
        stroke="currentColor" 
        stroke-width="2"
      >
        <polyline points="6 9 12 15 18 9"/>
      </svg>
    </button>
    
    <Transition name="dropdown">
      <div v-if="isOpen" class="dropdown">
        <button
          v-for="lang in languages"
          :key="lang.code"
          :class="['dropdown-item', { active: lang.code === locale }]"
          @click="changeLanguage(lang.code)"
        >
          <span class="flag">{{ lang.flag }}</span>
          <span class="lang-name">{{ lang.name }}</span>
          <svg 
            v-if="lang.code === locale"
            class="check-icon"
            width="14" 
            height="14" 
            viewBox="0 0 24 24" 
            fill="none" 
            stroke="currentColor" 
            stroke-width="2"
          >
            <polyline points="20 6 9 17 4 12"/>
          </svg>
        </button>
      </div>
    </Transition>
  </div>
</template>

<style scoped>
.language-selector {
  position: relative;
}

.selector-button {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  padding: var(--spacing-xs) var(--spacing-sm);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  font-family: inherit;
  font-size: 0.8125rem;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.selector-button:hover {
  background: var(--bg-elevated);
  border-color: var(--accent-primary);
  color: var(--text-primary);
}

.flag {
  font-size: 1rem;
  line-height: 1;
}

.lang-code {
  font-weight: 500;
  letter-spacing: 0.05em;
}

.chevron {
  transition: transform var(--transition-fast);
}

.chevron.open {
  transform: rotate(180deg);
}

.dropdown {
  position: absolute;
  top: calc(100% + var(--spacing-xs));
  right: 0;
  min-width: 160px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-lg);
  overflow: hidden;
  z-index: 1000;
}

.dropdown-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  width: 100%;
  padding: var(--spacing-sm) var(--spacing-md);
  background: transparent;
  border: none;
  color: var(--text-secondary);
  font-family: inherit;
  font-size: 0.875rem;
  text-align: left;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.dropdown-item:hover {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.dropdown-item.active {
  background: rgba(0, 212, 255, 0.1);
  color: var(--accent-primary);
}

.lang-name {
  flex: 1;
}

.check-icon {
  color: var(--accent-primary);
}

/* Dropdown animation */
.dropdown-enter-active,
.dropdown-leave-active {
  transition: all var(--transition-fast);
}

.dropdown-enter-from,
.dropdown-leave-to {
  opacity: 0;
  transform: translateY(-8px);
}
</style>



