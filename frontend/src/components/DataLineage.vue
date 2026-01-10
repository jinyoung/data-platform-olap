<script setup>
import { ref, computed, onMounted, watch, nextTick } from 'vue'
import { useI18n } from 'vue-i18n'

const { t } = useI18n()

// API Gateway URL
const API_GATEWAY_URL = import.meta.env.VITE_API_GATEWAY_URL ?? 'http://localhost:9000'
const API_BASE_URL = `${API_GATEWAY_URL}/olap/api`

// State
const loading = ref(false)
const error = ref(null)
const lineageData = ref(null)
const selectedNode = ref(null)
const searchQuery = ref('')

// Fetch lineage data from API
async function fetchLineageData() {
  loading.value = true
  error.value = null
  
  try {
    const response = await fetch(`${API_BASE_URL}/etl/lineage/overview`)
    if (!response.ok) throw new Error('Failed to fetch lineage data')
    lineageData.value = await response.json()
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// Fetch detailed lineage for a cube
async function fetchCubeLineage(cubeName) {
  try {
    const response = await fetch(`${API_BASE_URL}/etl/lineage/${cubeName}`)
    if (!response.ok) throw new Error('Failed to fetch cube lineage')
    return await response.json()
  } catch (e) {
    console.error('Failed to fetch cube lineage:', e)
    return null
  }
}

// On mount
onMounted(() => {
  fetchLineageData()
})

// Computed: filter nodes by search
const filteredSourceTables = computed(() => {
  if (!lineageData.value?.source_tables) return []
  if (!searchQuery.value) return lineageData.value.source_tables
  const q = searchQuery.value.toLowerCase()
  return lineageData.value.source_tables.filter(t => 
    t.name.toLowerCase().includes(q)
  )
})

const filteredTargetTables = computed(() => {
  if (!lineageData.value?.target_tables) return []
  if (!searchQuery.value) return lineageData.value.target_tables
  const q = searchQuery.value.toLowerCase()
  return lineageData.value.target_tables.filter(t => 
    t.name.toLowerCase().includes(q)
  )
})

const filteredETLProcesses = computed(() => {
  if (!lineageData.value?.etl_processes) return []
  if (!searchQuery.value) return lineageData.value.etl_processes
  const q = searchQuery.value.toLowerCase()
  return lineageData.value.etl_processes.filter(p => 
    p.name.toLowerCase().includes(q) || p.cube_name.toLowerCase().includes(q)
  )
})

// Get flows for a specific node
function getNodeFlows(nodeId) {
  if (!lineageData.value?.data_flows) return { incoming: [], outgoing: [] }
  return {
    incoming: lineageData.value.data_flows.filter(f => f.to === nodeId),
    outgoing: lineageData.value.data_flows.filter(f => f.from === nodeId)
  }
}

// Check if node is connected to selected
function isConnectedToSelected(nodeId) {
  if (!selectedNode.value) return false
  const flows = lineageData.value?.data_flows || []
  return flows.some(f => 
    (f.from === selectedNode.value && f.to === nodeId) ||
    (f.to === selectedNode.value && f.from === nodeId)
  )
}

// Select node
function selectNode(nodeId, type) {
  if (selectedNode.value === nodeId) {
    selectedNode.value = null
  } else {
    selectedNode.value = nodeId
  }
}

// Get icon for table type
function getTableIcon(type) {
  switch (type) {
    case 'fact': return '🎯'
    case 'dimension': return '📐'
    case 'source': return '📊'
    default: return '📋'
  }
}

// Get badge class for table type
function getBadgeClass(type) {
  switch (type) {
    case 'fact': return 'badge-fact'
    case 'dimension': return 'badge-dimension'
    case 'source': return 'badge-source'
    default: return ''
  }
}

// Has data check
const hasData = computed(() => {
  return lineageData.value && (
    lineageData.value.source_tables?.length > 0 ||
    lineageData.value.etl_processes?.length > 0 ||
    lineageData.value.target_tables?.length > 0
  )
})
</script>

<template>
  <div class="data-lineage">
    <!-- Header -->
    <div class="lineage-header">
      <div class="header-left">
        <h2>{{ t('lineage.title') }}</h2>
        <p class="subtitle">{{ t('lineage.subtitle') }}</p>
      </div>
      <div class="header-right">
        <div class="search-box">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="11" cy="11" r="8"/>
            <line x1="21" y1="21" x2="16.65" y2="16.65"/>
          </svg>
          <input 
            v-model="searchQuery" 
            type="text" 
            :placeholder="t('lineage.searchPlaceholder')"
          />
        </div>
        <button class="btn btn-primary" @click="fetchLineageData">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M23 4v6h-6"/>
            <path d="M1 20v-6h6"/>
            <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10"/>
            <path d="M20.49 15a9 9 0 0 1-14.85 3.36L1 14"/>
          </svg>
          {{ t('lineage.refresh') }}
        </button>
      </div>
    </div>

    <!-- Summary Stats -->
    <div class="stats-bar" v-if="lineageData?.summary">
      <div class="stat-item source">
        <span class="stat-value">{{ lineageData.summary.total_sources }}</span>
        <span class="stat-label">{{ t('lineage.sourceTables') }}</span>
      </div>
      <div class="stat-item etl">
        <span class="stat-value">{{ lineageData.summary.total_etl_processes }}</span>
        <span class="stat-label">{{ t('lineage.etlProcesses') }}</span>
      </div>
      <div class="stat-item target">
        <span class="stat-value">{{ lineageData.summary.total_targets }}</span>
        <span class="stat-label">{{ t('lineage.targetTables') }}</span>
      </div>
      <div class="stat-item flows">
        <span class="stat-value">{{ lineageData.summary.total_flows }}</span>
        <span class="stat-label">{{ t('lineage.dataFlows') }}</span>
      </div>
    </div>

    <!-- Loading State -->
    <div v-if="loading" class="loading-state">
      <div class="spinner"></div>
      <span>{{ t('common.loading') }}</span>
    </div>

    <!-- Error State -->
    <div v-else-if="error" class="error-state">
      <span class="error-icon">⚠️</span>
      <p>{{ error }}</p>
      <button class="btn btn-secondary" @click="fetchLineageData">{{ t('common.retry') }}</button>
    </div>

    <!-- Empty State -->
    <div v-else-if="!hasData" class="empty-state">
      <div class="empty-icon">🔗</div>
      <h3>{{ t('lineage.noData') }}</h3>
      <p>{{ t('lineage.noDataDesc') }}</p>
    </div>

    <!-- Lineage Diagram -->
    <div v-else class="lineage-diagram">
      <!-- Column Headers -->
      <div class="column-headers">
        <div class="column-header source">
          <span class="header-icon">📊</span>
          <span>{{ t('lineage.sourceTables') }}</span>
        </div>
        <div class="column-header etl">
          <span class="header-icon">⚙️</span>
          <span>{{ t('lineage.etlProcess') }}</span>
        </div>
        <div class="column-header target">
          <span class="header-icon">🎯</span>
          <span>{{ t('lineage.targetTables') }}</span>
        </div>
      </div>

      <!-- Lineage Flow -->
      <div class="lineage-flow">
        <!-- Source Tables Column -->
        <div class="flow-column source-column">
          <div 
            v-for="table in filteredSourceTables" 
            :key="table.id"
            class="node-card source-node"
            :class="{ 
              selected: selectedNode === table.id,
              connected: isConnectedToSelected(table.id),
              dimmed: selectedNode && !isConnectedToSelected(table.id) && selectedNode !== table.id
            }"
            @click="selectNode(table.id, 'source')"
          >
            <div class="node-header">
              <span class="node-icon">{{ getTableIcon('source') }}</span>
              <span class="node-name">{{ table.name }}</span>
              <span class="expand-icon">▤</span>
            </div>
            <div class="node-meta">
              <span class="meta-item">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <rect x="3" y="3" width="18" height="18" rx="2"/>
                  <line x1="3" y1="9" x2="21" y2="9"/>
                  <line x1="9" y1="21" x2="9" y2="9"/>
                </svg>
                {{ table.columns }} Columns
              </span>
            </div>
            <div class="node-badge" :class="getBadgeClass('source')">
              {{ table.schema || 'public' }}
            </div>
          </div>
        </div>

        <!-- ETL Process Column -->
        <div class="flow-column etl-column">
          <div 
            v-for="process in filteredETLProcesses" 
            :key="process.id"
            class="node-card etl-node"
            :class="{ 
              selected: selectedNode === process.id,
              connected: isConnectedToSelected(process.id),
              dimmed: selectedNode && !isConnectedToSelected(process.id) && selectedNode !== process.id
            }"
            @click="selectNode(process.id, 'etl')"
          >
            <div class="node-header">
              <span class="node-icon">⚙️</span>
              <span class="node-name">{{ process.name }}</span>
            </div>
            <div class="node-operation">
              <span class="operation-badge" :class="process.sync_mode">
                ⚡ {{ process.operation }}
              </span>
            </div>
            <div class="node-meta">
              <span class="meta-item">{{ process.mappings_count }} mappings</span>
            </div>
          </div>
        </div>

        <!-- Target Tables Column -->
        <div class="flow-column target-column">
          <div 
            v-for="table in filteredTargetTables" 
            :key="table.id"
            class="node-card target-node"
            :class="{ 
              selected: selectedNode === table.id,
              connected: isConnectedToSelected(table.id),
              dimmed: selectedNode && !isConnectedToSelected(table.id) && selectedNode !== table.id,
              'is-fact': table.type === 'fact',
              'is-dimension': table.type === 'dimension'
            }"
            @click="selectNode(table.id, 'target')"
          >
            <div class="node-header">
              <span class="node-icon">{{ getTableIcon(table.type) }}</span>
              <span class="node-name">{{ table.name }}</span>
              <span class="type-badge" :class="getBadgeClass(table.type)">
                {{ table.type === 'fact' ? t('lineage.target') : '' }}
              </span>
            </div>
            <div class="node-meta">
              <span class="meta-item">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <rect x="3" y="3" width="18" height="18" rx="2"/>
                  <line x1="3" y1="9" x2="21" y2="9"/>
                  <line x1="9" y1="21" x2="9" y2="9"/>
                </svg>
                {{ table.columns }} Columns
              </span>
            </div>
            <div class="node-badge" :class="getBadgeClass(table.type)">
              {{ table.schema }}/{{ table.cube_name }}
            </div>
          </div>
        </div>

        <!-- SVG Connections -->
        <svg class="flow-connections" preserveAspectRatio="none">
          <!-- Connection lines will be drawn dynamically -->
          <defs>
            <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
              <polygon points="0 0, 10 3.5, 0 7" fill="currentColor" />
            </marker>
          </defs>
        </svg>
      </div>

      <!-- Legend -->
      <div class="lineage-legend">
        <div class="legend-item">
          <span class="legend-icon source">●</span>
          <span>{{ t('lineage.sourceTable') }}</span>
        </div>
        <div class="legend-item">
          <span class="legend-icon etl">●</span>
          <span>{{ t('lineage.etlProcess') }}</span>
        </div>
        <div class="legend-item">
          <span class="legend-icon target">●</span>
          <span>{{ t('lineage.targetTable') }}</span>
        </div>
        <div class="legend-item">
          <span class="legend-line">- - -</span>
          <span>{{ t('lineage.dataFlow') }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.data-lineage {
  padding: var(--spacing-lg);
  height: 100%;
  display: flex;
  flex-direction: column;
}

/* Header */
.lineage-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: var(--spacing-lg);
}

.header-left h2 {
  font-size: 1.75rem;
  margin-bottom: var(--spacing-xs);
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.subtitle {
  color: var(--text-muted);
  font-size: 0.9375rem;
}

.header-right {
  display: flex;
  align-items: center;
  gap: var(--spacing-md);
}

.search-box {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-md);
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  color: var(--text-muted);
}

.search-box input {
  background: none;
  border: none;
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.875rem;
  min-width: 200px;
}

.search-box input:focus {
  outline: none;
}

.search-box input::placeholder {
  color: var(--text-muted);
}

/* Stats Bar */
.stats-bar {
  display: flex;
  gap: var(--spacing-lg);
  padding: var(--spacing-lg);
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  margin-bottom: var(--spacing-lg);
}

.stat-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: var(--spacing-md) var(--spacing-xl);
  border-radius: var(--radius-md);
  flex: 1;
}

.stat-item.source {
  background: rgba(59, 130, 246, 0.1);
  border: 1px solid rgba(59, 130, 246, 0.3);
}

.stat-item.etl {
  background: rgba(139, 92, 246, 0.1);
  border: 1px solid rgba(139, 92, 246, 0.3);
}

.stat-item.target {
  background: rgba(16, 185, 129, 0.1);
  border: 1px solid rgba(16, 185, 129, 0.3);
}

.stat-item.flows {
  background: rgba(0, 212, 255, 0.1);
  border: 1px solid rgba(0, 212, 255, 0.3);
}

.stat-value {
  font-size: 2rem;
  font-weight: 700;
}

.stat-item.source .stat-value { color: #3b82f6; }
.stat-item.etl .stat-value { color: #8b5cf6; }
.stat-item.target .stat-value { color: #10b981; }
.stat-item.flows .stat-value { color: var(--accent-primary); }

.stat-label {
  font-size: 0.75rem;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-top: var(--spacing-xs);
}

/* States */
.loading-state,
.error-state,
.empty-state {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-md);
  color: var(--text-muted);
}

.spinner {
  width: 40px;
  height: 40px;
  border: 3px solid var(--border-color);
  border-top-color: var(--accent-primary);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.empty-icon {
  font-size: 4rem;
  opacity: 0.5;
}

.empty-state h3 {
  color: var(--text-secondary);
}

.error-icon {
  font-size: 2rem;
}

.error-state p {
  color: var(--accent-error);
}

/* Lineage Diagram */
.lineage-diagram {
  flex: 1;
  display: flex;
  flex-direction: column;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  overflow: hidden;
}

/* Column Headers */
.column-headers {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: var(--spacing-lg);
  padding: var(--spacing-md) var(--spacing-lg);
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-color);
}

.column-header {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-sm);
  font-weight: 600;
  font-size: 0.9375rem;
}

.column-header.source { color: #3b82f6; }
.column-header.etl { color: #8b5cf6; }
.column-header.target { color: #10b981; }

.header-icon {
  font-size: 1.25rem;
}

/* Lineage Flow */
.lineage-flow {
  flex: 1;
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: var(--spacing-lg);
  padding: var(--spacing-lg);
  position: relative;
  overflow-y: auto;
}

.flow-column {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-md);
}

/* Node Cards */
.node-card {
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  padding: var(--spacing-md);
  cursor: pointer;
  transition: all 0.2s ease;
  position: relative;
}

.node-card:hover {
  border-color: var(--accent-primary);
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
}

.node-card.selected {
  border-color: var(--accent-primary);
  background: rgba(0, 212, 255, 0.1);
  box-shadow: 0 0 0 2px rgba(0, 212, 255, 0.3);
}

.node-card.connected {
  border-color: var(--accent-secondary);
  background: rgba(139, 92, 246, 0.05);
}

.node-card.dimmed {
  opacity: 0.4;
}

/* Source Node */
.source-node {
  border-left: 3px solid #3b82f6;
}

.source-node:hover,
.source-node.selected {
  border-left-color: #3b82f6;
}

/* ETL Node */
.etl-node {
  border-left: 3px solid #8b5cf6;
  background: linear-gradient(135deg, rgba(139, 92, 246, 0.1), transparent);
}

.etl-node:hover,
.etl-node.selected {
  border-left-color: #8b5cf6;
}

/* Target Node */
.target-node {
  border-left: 3px solid #10b981;
}

.target-node.is-fact {
  border-left-color: #10b981;
  background: linear-gradient(135deg, rgba(16, 185, 129, 0.15), transparent);
}

.target-node.is-dimension {
  border-left-color: #8b5cf6;
}

.node-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  margin-bottom: var(--spacing-xs);
}

.node-icon {
  font-size: 1.125rem;
}

.node-name {
  font-weight: 600;
  font-size: 0.9375rem;
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.expand-icon {
  color: var(--text-muted);
  font-size: 0.875rem;
}

.type-badge {
  font-size: 0.625rem;
  padding: 2px 6px;
  border-radius: 4px;
  font-weight: 600;
  text-transform: uppercase;
}

.badge-fact {
  background: rgba(16, 185, 129, 0.2);
  color: #10b981;
}

.badge-dimension {
  background: rgba(139, 92, 246, 0.2);
  color: #8b5cf6;
}

.badge-source {
  background: rgba(59, 130, 246, 0.2);
  color: #3b82f6;
}

.node-operation {
  margin: var(--spacing-xs) 0;
}

.operation-badge {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-xs);
  padding: var(--spacing-xs) var(--spacing-sm);
  background: rgba(139, 92, 246, 0.2);
  color: #8b5cf6;
  border-radius: var(--radius-sm);
  font-size: 0.75rem;
  font-weight: 600;
}

.operation-badge.incremental {
  background: rgba(16, 185, 129, 0.2);
  color: #10b981;
}

.node-meta {
  display: flex;
  align-items: center;
  gap: var(--spacing-md);
  margin-top: var(--spacing-xs);
}

.meta-item {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 0.75rem;
  color: var(--text-muted);
}

.node-badge {
  position: absolute;
  top: var(--spacing-sm);
  right: var(--spacing-sm);
  font-size: 0.625rem;
  padding: 2px 6px;
  border-radius: 4px;
  background: var(--bg-elevated);
  color: var(--text-muted);
}

/* Legend */
.lineage-legend {
  display: flex;
  gap: var(--spacing-xl);
  justify-content: center;
  padding: var(--spacing-md);
  background: var(--bg-tertiary);
  border-top: 1px solid var(--border-color);
}

.legend-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  font-size: 0.75rem;
  color: var(--text-muted);
}

.legend-icon {
  font-size: 0.875rem;
}

.legend-icon.source { color: #3b82f6; }
.legend-icon.etl { color: #8b5cf6; }
.legend-icon.target { color: #10b981; }

.legend-line {
  color: var(--accent-primary);
  letter-spacing: 2px;
}

/* Button Styles */
.btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-lg);
  border: none;
  border-radius: var(--radius-md);
  font-family: inherit;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-primary {
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  color: white;
}

.btn-primary:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(0, 212, 255, 0.3);
}

.btn-secondary {
  background: var(--bg-tertiary);
  color: var(--text-primary);
  border: 1px solid var(--border-color);
}

.btn-secondary:hover {
  background: var(--bg-elevated);
  border-color: var(--accent-primary);
}
</style>

