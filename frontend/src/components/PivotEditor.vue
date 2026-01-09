<script setup>
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useCubeStore } from '../store/cubeStore'
import draggable from 'vuedraggable'
import FieldList from './FieldList.vue'

const { t } = useI18n()
const store = useCubeStore()
const showSQL = ref(false)
const etlLoading = ref(false)
const etlResult = ref(null)
const showETLResult = ref(false)

// API Gateway URL - 모든 마이크로서비스 요청의 단일 진입점
const API_GATEWAY_URL = import.meta.env.VITE_API_GATEWAY_URL ?? 'http://localhost:9000'
const ETL_API_BASE = `${API_GATEWAY_URL}/olap/api`

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

// Execute ETL Sync
const executeETL = async () => {
  if (!store.currentCube) return
  
  etlLoading.value = true
  etlResult.value = null
  showETLResult.value = true
  
  try {
    const response = await fetch(`${ETL_API_BASE}/etl/sync/${store.currentCube}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force_full: false })
    })
    
    if (response.ok) {
      etlResult.value = await response.json()
    } else {
      etlResult.value = { status: 'failed', error: 'ETL 실행 실패' }
    }
  } catch (e) {
    etlResult.value = { status: 'failed', error: e.message }
  } finally {
    etlLoading.value = false
  }
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
        <h3>{{ t('pivot.configuration') }}</h3>
        <div class="config-actions">
          <button class="btn btn-etl" @click="executeETL" :disabled="etlLoading || !store.currentCube" title="OLTP → OLAP 데이터 동기화">
            <svg v-if="!etlLoading" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12 2v4m0 12v4M4.93 4.93l2.83 2.83m8.48 8.48l2.83 2.83M2 12h4m12 0h4M4.93 19.07l2.83-2.83m8.48-8.48l2.83-2.83"/>
            </svg>
            <span v-else class="spinner-sm"></span>
            {{ etlLoading ? 'ETL 중...' : '🔄 ETL 동기화' }}
          </button>
          <button class="btn btn-ghost" @click="showSQL = !showSQL">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polyline points="16 18 22 12 16 6"/>
              <polyline points="8 6 2 12 8 18"/>
            </svg>
            {{ t('pivot.showSql') }}
          </button>
          <button class="btn btn-secondary" @click="store.resetPivotConfig">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
              <path d="M3 3v5h5"/>
            </svg>
            {{ t('common.reset') }}
          </button>
          <button 
            class="btn btn-primary" 
            @click="executeQuery"
            :disabled="!store.pivotConfig.measures.length"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polygon points="5 3 19 12 5 21 5 3"/>
            </svg>
            {{ t('common.execute') }}
          </button>
        </div>
        
        <!-- ETL Result Popup -->
        <div v-if="showETLResult" class="etl-result-popup">
          <div class="etl-result-content" :class="etlResult?.status">
            <button class="close-btn" @click="showETLResult = false">×</button>
            <div v-if="etlLoading" class="etl-loading">
              <div class="spinner"></div>
              <span>ETL 동기화 진행 중...</span>
            </div>
            <div v-else-if="etlResult">
              <div v-if="etlResult.status === 'completed'" class="etl-success">
                <span class="icon">✅</span>
                <div>
                  <strong>ETL 동기화 완료!</strong>
                  <p>{{ etlResult.rows_inserted || 0 }}행 삽입, {{ etlResult.rows_updated || 0 }}행 업데이트</p>
                  <p class="duration">소요 시간: {{ etlResult.duration_ms }}ms</p>
                </div>
              </div>
              <div v-else class="etl-error">
                <span class="icon">❌</span>
                <div>
                  <strong>ETL 실패</strong>
                  <p>{{ etlResult.error }}</p>
                </div>
              </div>
            </div>
          </div>
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
            {{ t('pivot.rows') }}
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
            {{ t('pivot.columns') }}
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
            {{ t('pivot.measures') }}
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
          <span>{{ t('pivot.generatedSql') }}</span>
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

/* ETL Button */
.btn-etl {
  background: linear-gradient(135deg, #00d4ff, #7c3aed);
  color: white;
  border: none;
  padding: var(--spacing-sm) var(--spacing-md);
  border-radius: var(--radius-md);
  font-family: inherit;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  transition: all var(--transition-fast);
}

.btn-etl:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(0, 212, 255, 0.3);
}

.btn-etl:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.spinner-sm {
  width: 14px;
  height: 14px;
  border: 2px solid transparent;
  border-top-color: currentColor;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

/* ETL Result Popup */
.etl-result-popup {
  position: fixed;
  top: 80px;
  right: var(--spacing-lg);
  z-index: 1000;
  animation: slideIn 0.3s ease;
}

@keyframes slideIn {
  from {
    transform: translateX(100%);
    opacity: 0;
  }
  to {
    transform: translateX(0);
    opacity: 1;
  }
}

.etl-result-content {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
  min-width: 300px;
  box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
  position: relative;
}

.etl-result-content.completed {
  border-color: var(--accent-success);
}

.etl-result-content.failed {
  border-color: var(--accent-error);
}

.close-btn {
  position: absolute;
  top: var(--spacing-sm);
  right: var(--spacing-sm);
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  font-size: 1rem;
  cursor: pointer;
}

.close-btn:hover {
  background: var(--accent-error);
  border-color: var(--accent-error);
  color: white;
}

.etl-loading {
  display: flex;
  align-items: center;
  gap: var(--spacing-md);
  color: var(--accent-primary);
}

.etl-loading .spinner {
  width: 24px;
  height: 24px;
  border: 3px solid var(--border-color);
  border-top-color: var(--accent-primary);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

.etl-success,
.etl-error {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-md);
}

.etl-success .icon,
.etl-error .icon {
  font-size: 1.5rem;
}

.etl-success strong {
  color: var(--accent-success);
}

.etl-error strong {
  color: var(--accent-error);
}

.etl-success p,
.etl-error p {
  font-size: 0.875rem;
  color: var(--text-secondary);
  margin: var(--spacing-xs) 0 0 0;
}

.duration {
  font-size: 0.75rem;
  color: var(--text-muted);
}
</style>

