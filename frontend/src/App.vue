<script setup>
import { ref, onMounted } from 'vue'
import { useI18n } from 'vue-i18n'
import { useCubeStore } from './store/cubeStore'
import SchemaUpload from './components/SchemaUpload.vue'
import PivotEditor from './components/PivotEditor.vue'
import NaturalQuery from './components/NaturalQuery.vue'
import ResultGrid from './components/ResultGrid.vue'
import CubeModeler from './components/CubeModeler.vue'
import DataLineage from './components/DataLineage.vue'
import LanguageSelector from './components/LanguageSelector.vue'

const { t } = useI18n()
const store = useCubeStore()
const activeTab = ref('pivot')

onMounted(async () => {
  try {
    await store.loadCubes()
  } catch (e) {
    console.log('No cubes loaded yet')
  }
})
</script>

<template>
  <div class="app">
    <!-- Header -->
    <header class="app-header">
      <div class="header-left">
        <div class="logo">
          <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
            <rect x="2" y="2" width="12" height="12" rx="2" fill="url(#grad1)"/>
            <rect x="18" y="2" width="12" height="12" rx="2" fill="url(#grad2)"/>
            <rect x="2" y="18" width="12" height="12" rx="2" fill="url(#grad2)"/>
            <rect x="18" y="18" width="12" height="12" rx="2" fill="url(#grad3)"/>
            <defs>
              <linearGradient id="grad1" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stop-color="#00d4ff"/>
                <stop offset="100%" stop-color="#7c3aed"/>
              </linearGradient>
              <linearGradient id="grad2" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stop-color="#7c3aed"/>
                <stop offset="100%" stop-color="#f472b6"/>
              </linearGradient>
              <linearGradient id="grad3" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stop-color="#f472b6"/>
                <stop offset="100%" stop-color="#00d4ff"/>
              </linearGradient>
            </defs>
          </svg>
          <h1>{{ t('app.title') }}</h1>
        </div>
      </div>
      
      <div class="header-center">
        <nav class="tab-nav">
          <button 
            :class="['tab-btn', { active: activeTab === 'pivot' }]"
            @click="activeTab = 'pivot'"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <rect x="3" y="3" width="7" height="7"/>
              <rect x="14" y="3" width="7" height="7"/>
              <rect x="14" y="14" width="7" height="7"/>
              <rect x="3" y="14" width="7" height="7"/>
            </svg>
            {{ t('nav.pivotAnalysis') }}
          </button>
          <button 
            :class="['tab-btn', { active: activeTab === 'natural' }]"
            @click="activeTab = 'natural'"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
            </svg>
            {{ t('nav.naturalLanguage') }}
          </button>
          <button 
            :class="['tab-btn', { active: activeTab === 'modeler' }]"
            @click="activeTab = 'modeler'"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
              <polyline points="3.27 6.96 12 12.01 20.73 6.96"/>
              <line x1="12" y1="22.08" x2="12" y2="12"/>
            </svg>
            {{ t('nav.cubeModeler') }}
          </button>
          <button 
            :class="['tab-btn', { active: activeTab === 'lineage' }]"
            @click="activeTab = 'lineage'"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="5" cy="6" r="3"/>
              <circle cx="19" cy="6" r="3"/>
              <circle cx="12" cy="18" r="3"/>
              <line x1="8" y1="6" x2="16" y2="6"/>
              <line x1="6.5" y1="8.5" x2="10.5" y2="15.5"/>
              <line x1="17.5" y1="8.5" x2="13.5" y2="15.5"/>
            </svg>
            {{ t('nav.dataLineage') }}
          </button>
        </nav>
      </div>
      
      <div class="header-right">
        <LanguageSelector />
        <div v-if="store.currentCube" class="cube-badge">
          <span class="cube-icon">◈</span>
          {{ store.currentCube }}
        </div>
      </div>
    </header>
    
    <!-- Main Content -->
    <main :class="['app-main', { 'full-width': activeTab === 'modeler' || activeTab === 'lineage' }]">
      <!-- Sidebar - Schema Upload & Cube Selection (hidden in modeler and lineage) -->
      <aside v-if="activeTab !== 'modeler' && activeTab !== 'lineage'" class="sidebar">
        <SchemaUpload />
        
        <div v-if="store.hasCubes" class="cube-selector">
          <h3>{{ t('schema.availableCubes') }}</h3>
          <div class="cube-list">
            <button
              v-for="cube in store.cubes"
              :key="cube"
              :class="['cube-item', { active: store.currentCube === cube }]"
              @click="store.selectCube(cube)"
            >
              <span class="cube-icon">◈</span>
              {{ cube }}
            </button>
          </div>
        </div>
      </aside>
      
      <!-- Content Area -->
      <div class="content">
        <!-- Pivot Tab -->
        <div v-if="activeTab === 'pivot'" class="tab-content slide-up">
          <PivotEditor v-if="store.hasCubes" />
          <div v-else class="empty-state">
            <div class="empty-icon">📊</div>
            <h3>{{ t('schema.noSchema') }}</h3>
            <p>{{ t('schema.noSchemaDesc') }}</p>
          </div>
        </div>
        
        <!-- Natural Language Tab -->
        <div v-if="activeTab === 'natural'" class="tab-content slide-up">
          <NaturalQuery v-if="store.hasCubes" />
          <div v-else class="empty-state">
            <div class="empty-icon">💬</div>
            <h3>{{ t('schema.noSchema') }}</h3>
            <p>{{ t('schema.noSchemaDesc') }}</p>
          </div>
        </div>
        
        <!-- Cube Modeler Tab (includes ETL Design) -->
        <div v-if="activeTab === 'modeler'" class="tab-content slide-up">
          <CubeModeler />
        </div>
        
        <!-- Data Lineage Tab -->
        <div v-if="activeTab === 'lineage'" class="tab-content slide-up">
          <DataLineage />
        </div>
      </div>
    </main>
    
    <!-- Result Panel (hidden in modeler and lineage) -->
    <div v-if="store.queryResult && activeTab !== 'modeler' && activeTab !== 'lineage'" class="result-panel slide-up">
      <ResultGrid 
        :result="store.queryResult" 
        :sql="store.generatedSQL" 
        :pivotConfig="activeTab === 'pivot' ? store.pivotConfig : null"
      />
    </div>
    
    <!-- Error Toast -->
    <div v-if="store.error" class="error-toast fade-in">
      <span class="error-icon">⚠️</span>
      {{ store.error }}
      <button class="close-btn" @click="store.error = null">×</button>
    </div>
    
    <!-- Loading Overlay -->
    <div v-if="store.loading" class="loading-overlay">
      <div class="loading">
        <div class="spinner"></div>
        <span>{{ t('common.loading') }}</span>
      </div>
    </div>
    
    <!-- Footer -->
    <footer class="app-footer">
      <div class="footer-content">
        <span class="copyright">© {{ new Date().getFullYear() }} </span>
        <a href="https://www.uengine.io" target="_blank" rel="noopener noreferrer" class="footer-link">
          <svg class="uengine-logo" width="20" height="20" viewBox="0 0 24 24" fill="none">
            <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/>
            <path d="M8 12a4 4 0 0 0 8 0" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            <circle cx="9" cy="9" r="1.5" fill="currentColor"/>
            <circle cx="15" cy="9" r="1.5" fill="currentColor"/>
          </svg>
          <span>uEngine</span>
        </a>
        <span class="separator">•</span>
        <span class="product-name">{{ t('app.title') }}</span>
      </div>
    </footer>
  </div>
</template>

<style scoped>
.app {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  background: 
    radial-gradient(ellipse at 0% 0%, rgba(124, 58, 237, 0.1) 0%, transparent 50%),
    radial-gradient(ellipse at 100% 100%, rgba(0, 212, 255, 0.1) 0%, transparent 50%),
    var(--bg-primary);
}

/* Header */
.app-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-md) var(--spacing-xl);
  background: rgba(17, 24, 39, 0.8);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid var(--border-color);
  position: sticky;
  top: 0;
  z-index: 100;
}

.header-left {
  flex: 1;
}

.logo {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
}

.logo h1 {
  font-size: 1.25rem;
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.header-center {
  flex: 2;
  display: flex;
  justify-content: center;
}

.tab-nav {
  display: flex;
  gap: var(--spacing-xs);
  background: var(--bg-tertiary);
  padding: var(--spacing-xs);
  border-radius: var(--radius-lg);
}

.tab-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-lg);
  background: transparent;
  border: none;
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  font-family: inherit;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.tab-btn:hover {
  color: var(--text-primary);
}

.tab-btn.active {
  background: var(--bg-elevated);
  color: var(--accent-primary);
  box-shadow: var(--shadow-sm);
}

.header-right {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--spacing-md);
}

.cube-badge {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-md);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  font-size: 0.875rem;
  color: var(--accent-primary);
}

.cube-icon {
  color: var(--accent-secondary);
}

/* Main Layout */
.app-main {
  display: flex;
  flex: 1;
  gap: var(--spacing-lg);
  padding: var(--spacing-lg);
}

.app-main.full-width {
  /* Full width for Cube Modeler */
}

.app-main.full-width .content {
  max-width: 100%;
}

.sidebar {
  width: 280px;
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
}

.content {
  flex: 1;
  min-width: 0;
}

.tab-content {
  height: 100%;
}

/* Cube Selector */
.cube-selector {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
}

.cube-selector h3 {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: var(--spacing-md);
}

.cube-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-xs);
}

.cube-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-md);
  background: var(--bg-tertiary);
  border: 1px solid transparent;
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  font-family: inherit;
  font-size: 0.875rem;
  cursor: pointer;
  transition: all var(--transition-fast);
  text-align: left;
}

.cube-item:hover {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.cube-item.active {
  background: rgba(0, 212, 255, 0.1);
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

/* Empty State */
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 400px;
  text-align: center;
  color: var(--text-muted);
}

.empty-icon {
  font-size: 4rem;
  margin-bottom: var(--spacing-lg);
  opacity: 0.5;
}

.empty-state h3 {
  color: var(--text-secondary);
  margin-bottom: var(--spacing-sm);
}

/* Result Panel */
.result-panel {
  padding: 0 var(--spacing-lg) var(--spacing-lg);
}

/* Error Toast */
.error-toast {
  position: fixed;
  bottom: var(--spacing-lg);
  right: var(--spacing-lg);
  display: flex;
  align-items: center;
  gap: var(--spacing-md);
  padding: var(--spacing-md) var(--spacing-lg);
  background: var(--bg-secondary);
  border: 1px solid var(--accent-error);
  border-radius: var(--radius-md);
  color: var(--accent-error);
  box-shadow: var(--shadow-lg);
  z-index: 1000;
}

.close-btn {
  background: none;
  border: none;
  color: inherit;
  font-size: 1.25rem;
  cursor: pointer;
  opacity: 0.7;
}

.close-btn:hover {
  opacity: 1;
}

/* Loading Overlay */
.loading-overlay {
  position: fixed;
  inset: 0;
  background: rgba(10, 14, 23, 0.8);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  backdrop-filter: blur(4px);
}

/* Footer */
.app-footer {
  margin-top: auto;
  padding: var(--spacing-md) var(--spacing-xl);
  background: var(--bg-secondary);
  border-top: 1px solid var(--border-color);
}

.footer-content {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-sm);
  font-size: 0.8125rem;
  color: var(--text-muted);
}

.copyright {
  color: var(--text-muted);
}

.footer-link {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  color: var(--accent-primary);
  text-decoration: none;
  font-weight: 500;
  transition: all var(--transition-fast);
}

.footer-link:hover {
  color: var(--accent-secondary);
  text-shadow: 0 0 10px rgba(0, 212, 255, 0.5);
}

.uengine-logo {
  color: inherit;
}

.separator {
  color: var(--border-color);
}

.product-name {
  color: var(--text-secondary);
  font-weight: 500;
}
</style>
