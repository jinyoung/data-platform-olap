<script setup>
import { ref, watch } from 'vue'
import { useCubeStore } from '../store/cubeStore'
import draggable from 'vuedraggable'
import FieldList from './FieldList.vue'

const store = useCubeStore()
const showSQL = ref(false)

// Auto-preview SQL when config changes
watch(
  () => store.pivotConfig,
  async () => {
    if (store.pivotConfig.rows.length || store.pivotConfig.columns.length || store.pivotConfig.measures.length) {
      await store.previewSQL()
    }
  },
  { deep: true }
)

const executeQuery = async () => {
  await store.executePivotQuery()
}
</script>

<template>
  <div class="pivot-editor">
    <!-- Field List Sidebar -->
    <div class="fields-panel">
      <FieldList />
    </div>
    
    <!-- Pivot Configuration -->
    <div class="config-panel">
      <div class="config-header">
        <h3>Pivot Configuration</h3>
        <div class="config-actions">
          <button class="btn btn-ghost" @click="showSQL = !showSQL">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polyline points="16 18 22 12 16 6"/>
              <polyline points="8 6 2 12 8 18"/>
            </svg>
            {{ showSQL ? 'Hide' : 'Show' }} SQL
          </button>
          <button class="btn btn-secondary" @click="store.resetPivotConfig">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
              <path d="M3 3v5h5"/>
            </svg>
            Reset
          </button>
          <button 
            class="btn btn-primary" 
            @click="executeQuery"
            :disabled="!store.pivotConfig.measures.length"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polygon points="5 3 19 12 5 21 5 3"/>
            </svg>
            Execute
          </button>
        </div>
      </div>
      
      <!-- Drop Zones -->
      <div class="drop-zones">
        <!-- Rows -->
        <div class="zone-container">
          <label class="zone-label">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="3" y1="12" x2="21" y2="12"/>
              <line x1="3" y1="6" x2="21" y2="6"/>
              <line x1="3" y1="18" x2="21" y2="18"/>
            </svg>
            Rows
          </label>
          <draggable
            v-model="store.pivotConfig.rows"
            group="fields"
            item-key="level"
            class="drop-zone"
          >
            <template #item="{ element, index }">
              <div class="tag dimension">
                {{ element.dimension }} › {{ element.level }}
                <span class="remove" @click="store.removeFromRows(index)">×</span>
              </div>
            </template>
          </draggable>
        </div>
        
        <!-- Columns -->
        <div class="zone-container">
          <label class="zone-label">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="12" y1="3" x2="12" y2="21"/>
              <line x1="6" y1="3" x2="6" y2="21"/>
              <line x1="18" y1="3" x2="18" y2="21"/>
            </svg>
            Columns
          </label>
          <draggable
            v-model="store.pivotConfig.columns"
            group="fields"
            item-key="level"
            class="drop-zone"
          >
            <template #item="{ element, index }">
              <div class="tag dimension">
                {{ element.dimension }} › {{ element.level }}
                <span class="remove" @click="store.removeFromColumns(index)">×</span>
              </div>
            </template>
          </draggable>
        </div>
        
        <!-- Measures -->
        <div class="zone-container">
          <label class="zone-label">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="12" y1="20" x2="12" y2="10"/>
              <line x1="18" y1="20" x2="18" y2="4"/>
              <line x1="6" y1="20" x2="6" y2="16"/>
            </svg>
            Measures
          </label>
          <draggable
            v-model="store.pivotConfig.measures"
            group="measures"
            item-key="name"
            class="drop-zone"
          >
            <template #item="{ element, index }">
              <div class="tag measure">
                {{ element.name }}
                <span class="remove" @click="store.removeMeasure(index)">×</span>
              </div>
            </template>
          </draggable>
        </div>
      </div>
      
      <!-- SQL Preview -->
      <div v-if="showSQL && store.generatedSQL" class="sql-preview fade-in">
        <div class="sql-header">
          <span>Generated SQL</span>
          <button class="btn btn-ghost" @click="navigator.clipboard.writeText(store.generatedSQL)">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <rect x="9" y="9" width="13" height="13" rx="2"/>
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
            </svg>
            Copy
          </button>
        </div>
        <pre class="code-block">{{ store.generatedSQL }}</pre>
      </div>
    </div>
  </div>
</template>

<style scoped>
.pivot-editor {
  display: grid;
  grid-template-columns: 260px 1fr;
  gap: var(--spacing-lg);
  height: 100%;
}

.fields-panel {
  max-height: calc(100vh - 200px);
  overflow-y: auto;
}

.config-panel {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
}

.config-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.config-header h3 {
  font-size: 1rem;
}

.config-actions {
  display: flex;
  gap: var(--spacing-sm);
}

.drop-zones {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: var(--spacing-md);
}

.zone-container {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.zone-label {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  font-weight: 500;
}

.drop-zone {
  min-height: 100px;
  background: var(--bg-tertiary);
  border: 2px dashed var(--border-color);
  border-radius: var(--radius-md);
  padding: var(--spacing-md);
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-sm);
  align-content: flex-start;
  transition: all var(--transition-fast);
}

.drop-zone:empty::before {
  content: 'Drop fields here';
  color: var(--text-muted);
  font-size: 0.75rem;
  width: 100%;
  text-align: center;
  padding: var(--spacing-lg);
}

.sql-preview {
  border-top: 1px solid var(--border-color);
  padding-top: var(--spacing-lg);
}

.sql-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: var(--spacing-sm);
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
}

.code-block {
  max-height: 200px;
  overflow: auto;
}
</style>

