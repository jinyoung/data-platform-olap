<script setup>
import { ref, onMounted } from 'vue'
import { useCubeStore } from './store/cubeStore'
import SchemaUpload from './components/SchemaUpload.vue'
import PivotEditor from './components/PivotEditor.vue'
import NaturalQuery from './components/NaturalQuery.vue'
import ResultGrid from './components/ResultGrid.vue'

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
          <h1>AI Pivot Studio</h1>
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
            Pivot Analysis
          </button>
          <button 
            :class="['tab-btn', { active: activeTab === 'natural' }]"
            @click="activeTab = 'natural'"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
            </svg>
            Natural Language
          </button>
        </nav>
      </div>
      
      <div class="header-right">
        <div v-if="store.currentCube" class="cube-badge">
          <span class="cube-icon">◈</span>
          {{ store.currentCube }}
        </div>
      </div>
    </header>
    
    <!-- Main Content -->
    <main class="app-main">
      <!-- Sidebar - Schema Upload & Cube Selection -->
      <aside class="sidebar">
        <SchemaUpload />
        
        <div v-if="store.hasCubes" class="cube-selector">
          <h3>Available Cubes</h3>
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
            <h3>No Schema Loaded</h3>
            <p>Upload a Mondrian XML schema to get started with pivot analysis.</p>
          </div>
        </div>
        
        <!-- Natural Language Tab -->
        <div v-if="activeTab === 'natural'" class="tab-content slide-up">
          <NaturalQuery v-if="store.hasCubes" />
          <div v-else class="empty-state">
            <div class="empty-icon">💬</div>
            <h3>No Schema Loaded</h3>
            <p>Upload a Mondrian XML schema to use natural language queries.</p>
          </div>
        </div>
      </div>
    </main>
    
    <!-- Result Panel -->
    <div v-if="store.queryResult" class="result-panel slide-up">
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
        <span>Processing...</span>
      </div>
    </div>
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
  justify-content: flex-end;
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
</style>
