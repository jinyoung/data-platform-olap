<script setup>
import { ref, computed } from 'vue'

const props = defineProps({
  result: {
    type: Object,
    required: true
  },
  sql: {
    type: String,
    default: ''
  }
})

const showSQL = ref(false)
const sortColumn = ref(null)
const sortDirection = ref('asc')

const sortedRows = computed(() => {
  if (!props.result?.rows || !sortColumn.value) {
    return props.result?.rows || []
  }
  
  return [...props.result.rows].sort((a, b) => {
    const aVal = a[sortColumn.value]
    const bVal = b[sortColumn.value]
    
    if (aVal === bVal) return 0
    
    const comparison = aVal < bVal ? -1 : 1
    return sortDirection.value === 'asc' ? comparison : -comparison
  })
})

const toggleSort = (column) => {
  if (sortColumn.value === column) {
    sortDirection.value = sortDirection.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortColumn.value = column
    sortDirection.value = 'asc'
  }
}

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
    <div v-else-if="result.rows?.length" class="table-container">
      <table>
        <thead>
          <tr>
            <th 
              v-for="col in result.columns" 
              :key="col"
              @click="toggleSort(col)"
              class="sortable"
            >
              {{ col }}
              <span v-if="sortColumn === col" class="sort-indicator">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in sortedRows" :key="index">
            <td v-for="col in result.columns" :key="col">
              {{ formatValue(row[col]) }}
            </td>
          </tr>
        </tbody>
      </table>
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

.table-container {
  max-height: 400px;
  overflow: auto;
}

table {
  min-width: 100%;
}

th {
  position: sticky;
  top: 0;
  z-index: 10;
}

th.sortable {
  cursor: pointer;
  user-select: none;
}

th.sortable:hover {
  background: var(--bg-tertiary);
}

.sort-indicator {
  margin-left: var(--spacing-xs);
  color: var(--accent-primary);
}

td {
  white-space: nowrap;
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

