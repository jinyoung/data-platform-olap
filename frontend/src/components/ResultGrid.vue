<script setup>
import { ref, computed } from 'vue'
import PivotTable from './PivotTable.vue'

const props = defineProps({
  result: {
    type: Object,
    required: true
  },
  sql: {
    type: String,
    default: ''
  },
  pivotConfig: {
    type: Object,
    default: null
  }
})

const showSQL = ref(false)
const viewMode = ref('pivot') // 'pivot' or 'flat'

// Check if pivot view is available
const canShowPivot = computed(() => {
  return props.pivotConfig?.columns?.length > 0
})

const formatValue = (value) => {
  if (value === null || value === undefined) return '-'
  if (typeof value === 'number') {
    return value.toLocaleString()
  }
  return String(value)
}

const copySQL = () => {
  navigator.clipboard.writeText(props.sql)
}

const exportCSV = () => {
  if (!props.result?.rows?.length) return
  
  const headers = props.result.columns.join(',')
  const rows = props.result.rows.map(row => 
    props.result.columns.map(col => {
      const val = row[col]
      if (typeof val === 'string' && val.includes(',')) {
        return `"${val}"`
      }
      return val
    }).join(',')
  ).join('\n')
  
  const csv = `${headers}\n${rows}`
  const blob = new Blob([csv], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'query-result.csv'
  a.click()
  URL.revokeObjectURL(url)
}
</script>

<template>
  <div class="result-grid">
    <!-- Header -->
    <div class="result-header">
      <div class="result-info">
        <h3>Query Results</h3>
        <div class="result-meta">
          <span class="meta-item">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <rect x="3" y="3" width="18" height="18" rx="2"/>
              <path d="M3 9h18"/>
              <path d="M9 21V9"/>
            </svg>
            {{ result.row_count || 0 }} rows
          </span>
          <span class="meta-item">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="12" cy="12" r="10"/>
              <polyline points="12 6 12 12 16 14"/>
            </svg>
            {{ result.execution_time_ms?.toFixed(2) || 0 }}ms
          </span>
        </div>
      </div>
      
      <div class="result-actions">
        <!-- View Mode Toggle (only show if pivot columns exist) -->
        <div v-if="canShowPivot" class="view-toggle">
          <button 
            :class="['toggle-btn', { active: viewMode === 'pivot' }]"
            @click="viewMode = 'pivot'"
            title="Pivot View"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <rect x="3" y="3" width="7" height="7"/>
              <rect x="14" y="3" width="7" height="7"/>
              <rect x="14" y="14" width="7" height="7"/>
              <rect x="3" y="14" width="7" height="7"/>
            </svg>
          </button>
          <button 
            :class="['toggle-btn', { active: viewMode === 'flat' }]"
            @click="viewMode = 'flat'"
            title="Flat Table View"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="3" y1="6" x2="21" y2="6"/>
              <line x1="3" y1="12" x2="21" y2="12"/>
              <line x1="3" y1="18" x2="21" y2="18"/>
            </svg>
          </button>
        </div>
        
        <button class="btn btn-ghost" @click="showSQL = !showSQL">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <polyline points="16 18 22 12 16 6"/>
            <polyline points="8 6 2 12 8 18"/>
          </svg>
          {{ showSQL ? 'Hide' : 'Show' }} SQL
        </button>
        <button class="btn btn-ghost" @click="copySQL" v-if="sql">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <rect x="9" y="9" width="13" height="13" rx="2"/>
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
          </svg>
          Copy SQL
        </button>
        <button class="btn btn-secondary" @click="exportCSV" :disabled="!result.rows?.length">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
            <polyline points="7 10 12 15 17 10"/>
            <line x1="12" y1="15" x2="12" y2="3"/>
          </svg>
          Export CSV
        </button>
      </div>
    </div>
    
    <!-- SQL Preview -->
    <div v-if="showSQL && sql" class="sql-section fade-in">
      <pre class="code-block">{{ sql }}</pre>
    </div>
    
    <!-- Error State -->
    <div v-if="result.error" class="error-state">
      <div class="error-icon">⚠️</div>
      <h4>Query Error</h4>
      <p>{{ result.error }}</p>
    </div>
    
    <!-- Data Table -->
    <div v-else-if="result.rows?.length" class="table-section">
      <!-- Pivot Table View -->
      <PivotTable 
        v-if="canShowPivot && viewMode === 'pivot'"
        :result="result"
        :pivotConfig="pivotConfig"
      />
      
      <!-- Flat Table View -->
      <div v-else class="table-container">
        <table>
          <thead>
            <tr>
              <th v-for="col in result.columns" :key="col">
                {{ col }}
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, index) in result.rows" :key="index">
              <td v-for="col in result.columns" :key="col">
                {{ formatValue(row[col]) }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
    
    <!-- Empty State -->
    <div v-else class="empty-state">
      <div class="empty-icon">📭</div>
      <h4>No Results</h4>
      <p>The query returned no data.</p>
    </div>
  </div>
</template>

<style scoped>
.result-grid {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  overflow: hidden;
}

.result-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: var(--spacing-md) var(--spacing-lg);
  background: var(--bg-elevated);
  border-bottom: 1px solid var(--border-color);
}

.result-info h3 {
  font-size: 1rem;
  margin-bottom: var(--spacing-xs);
}

.result-meta {
  display: flex;
  gap: var(--spacing-md);
}

.meta-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  font-size: 0.75rem;
  color: var(--text-muted);
}

.result-actions {
  display: flex;
  gap: var(--spacing-sm);
  align-items: center;
}

/* View Toggle */
.view-toggle {
  display: flex;
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
  padding: 2px;
  margin-right: var(--spacing-sm);
}

.toggle-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  background: transparent;
  border: none;
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.toggle-btn:hover {
  color: var(--text-primary);
}

.toggle-btn.active {
  background: var(--bg-elevated);
  color: var(--accent-primary);
  box-shadow: var(--shadow-sm);
}

.sql-section {
  padding: var(--spacing-md) var(--spacing-lg);
  border-bottom: 1px solid var(--border-color);
  background: var(--bg-primary);
}

.sql-section .code-block {
  margin: 0;
  max-height: 150px;
}

.table-section {
  padding: var(--spacing-md);
}

.table-container {
  max-height: 400px;
  overflow: auto;
  border-radius: var(--radius-md);
  border: 1px solid var(--border-color);
}

table {
  min-width: 100%;
  border-collapse: collapse;
}

th {
  background: var(--bg-elevated);
  color: var(--accent-primary);
  font-weight: 500;
  text-transform: uppercase;
  font-size: 0.6875rem;
  letter-spacing: 0.05em;
  padding: var(--spacing-sm) var(--spacing-md);
  text-align: left;
  position: sticky;
  top: 0;
  z-index: 10;
  border-bottom: 2px solid var(--accent-primary);
}

td {
  padding: var(--spacing-sm) var(--spacing-md);
  border-bottom: 1px solid var(--border-color);
  white-space: nowrap;
}

tr:hover td {
  background: var(--bg-tertiary);
}

.error-state,
.empty-state {
  padding: var(--spacing-xl);
  text-align: center;
  color: var(--text-muted);
}

.error-state {
  color: var(--accent-error);
}

.error-icon,
.empty-icon {
  font-size: 2.5rem;
  margin-bottom: var(--spacing-md);
}

.error-state h4,
.empty-state h4 {
  margin-bottom: var(--spacing-sm);
}

.error-state p {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8125rem;
  background: var(--bg-primary);
  padding: var(--spacing-md);
  border-radius: var(--radius-md);
  text-align: left;
  max-width: 600px;
  margin: 0 auto;
}
</style>
