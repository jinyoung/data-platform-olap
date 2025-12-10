<script setup>
import { computed, ref } from 'vue'
import { useCubeStore } from '../store/cubeStore'

const props = defineProps({
  result: {
    type: Object,
    required: true
  },
  pivotConfig: {
    type: Object,
    required: true
  }
})

const store = useCubeStore()

// Sorting state
const sortConfig = ref({
  colPath: null,      // Which column (leaf column fullPath)
  measure: null,      // Which measure
  direction: 'desc'   // 'asc' or 'desc'
})

// Check if we have column dimensions (need pivot transformation)
const hasPivotColumns = computed(() => {
  return props.pivotConfig?.columns?.length > 0
})

// Get the effective config (with drill-downs applied)
const effectiveConfig = computed(() => {
  return store.buildDrillDownConfig()
})

// Get column keys from effective config
const rowKeys = computed(() => {
  return effectiveConfig.value?.rows?.map(r => ({
    key: `${r.dimension}_${r.level}`.toLowerCase(),
    dimension: r.dimension,
    level: r.level
  })) || []
})

const colKeys = computed(() => {
  return effectiveConfig.value?.columns?.map(c => ({
    key: `${c.dimension}_${c.level}`.toLowerCase(),
    dimension: c.dimension,
    level: c.level
  })) || []
})

const measureKeys = computed(() => {
  return props.pivotConfig?.measures?.map(m => m.name.toLowerCase()) || []
})

// Build hierarchical column structure for headers
const columnHeaders = computed(() => {
  if (!hasPivotColumns.value || !props.result?.rows) return []
  
  const headers = []
  const numMeasures = measureKeys.value.length || 1
  
  // For each column level, get unique values with their parent paths
  colKeys.value.forEach((col, levelIdx) => {
    const valuesMap = new Map()
    
    props.result.rows.forEach(row => {
      const val = row[col.key]
      if (val === undefined || val === null) return
      
      // Build parent path for grouping (all ancestor values joined)
      const parentValues = colKeys.value
        .slice(0, levelIdx)
        .map(c => row[c.key])
      const parentPath = parentValues.join('|')
      
      // Unique key includes parent path to handle same values under different parents
      const uniqueKey = parentPath ? `${parentPath}|${val}` : String(val)
      
      if (!valuesMap.has(uniqueKey)) {
        valuesMap.set(uniqueKey, {
          value: val,
          parentPath: parentPath,
          fullPath: uniqueKey,
          dimension: col.dimension,
          level: col.level,
          canDrillDown: store.hasNextLevel(col.dimension, col.level),
          isExpanded: store.isExpanded('col', col.dimension, col.level, val),
          colspan: numMeasures // Default colspan is number of measures
        })
      }
    })
    
    headers.push({
      level: levelIdx,
      dimension: col.dimension,
      levelName: col.level,
      values: Array.from(valuesMap.values()).sort((a, b) => {
        // Sort by parent path first, then by value
        if (a.parentPath !== b.parentPath) {
          return a.parentPath.localeCompare(b.parentPath)
        }
        return String(a.value).localeCompare(String(b.value))
      })
    })
  })
  
  // Calculate colspans from bottom to top
  // Each parent's colspan = sum of its children's colspans
  for (let i = headers.length - 2; i >= 0; i--) {
    const parentLevel = headers[i]
    const childLevel = headers[i + 1]
    
    parentLevel.values.forEach(parent => {
      // Find all children whose parentPath matches this parent's fullPath
      const children = childLevel.values.filter(child => 
        child.parentPath === parent.fullPath
      )
      
      if (children.length > 0) {
        // Sum up children's colspans
        parent.colspan = children.reduce((sum, child) => sum + child.colspan, 0)
      } else {
        // No children found, use default (measures count)
        parent.colspan = numMeasures
      }
    })
  }
  
  return headers
})

// Get flat list of leaf column values for data cells
const leafColumns = computed(() => {
  if (columnHeaders.value.length === 0) return []
  
  const lastLevel = columnHeaders.value[columnHeaders.value.length - 1]
  return lastLevel?.values || []
})

// Build pivot data structure
const pivotData = computed(() => {
  if (!hasPivotColumns.value || !props.result?.rows) {
    return null
  }
  
  const data = new Map()
  
  props.result.rows.forEach(row => {
    // Build row key from all row dimensions
    const rowKeyValue = rowKeys.value.map(k => row[k.key]).join('|')
    
    // Build column key from all column dimensions
    const colKeyValue = colKeys.value.map(c => row[c.key]).join('|')
    
    if (!data.has(rowKeyValue)) {
      // Store row dimension values
      const rowDimValues = {}
      rowKeys.value.forEach(k => {
        rowDimValues[k.key] = {
          value: row[k.key],
          dimension: k.dimension,
          level: k.level,
          canDrillDown: store.hasNextLevel(k.dimension, k.level),
          isExpanded: store.isExpanded('row', k.dimension, k.level, row[k.key])
        }
      })
      data.set(rowKeyValue, {
        rowKey: rowKeyValue,
        rowDims: rowDimValues,
        cells: new Map()
      })
    }
    
    // Store measure values for this cell
    const measures = {}
    measureKeys.value.forEach(m => {
      measures[m] = row[m]
    })
    data.get(rowKeyValue).cells.set(colKeyValue, measures)
  })
  
  return data
})

// Get unique row entries for rendering (with sorting applied)
const pivotRows = computed(() => {
  if (!pivotData.value) return []
  
  let rows = Array.from(pivotData.value.values())
  
  // Apply sorting if configured
  if (sortConfig.value.colPath && sortConfig.value.measure) {
    rows = rows.sort((a, b) => {
      const aCell = a.cells.get(sortConfig.value.colPath)
      const bCell = b.cells.get(sortConfig.value.colPath)
      
      const aVal = aCell ? parseFloat(aCell[sortConfig.value.measure]) || 0 : 0
      const bVal = bCell ? parseFloat(bCell[sortConfig.value.measure]) || 0 : 0
      
      const diff = aVal - bVal
      return sortConfig.value.direction === 'asc' ? diff : -diff
    })
  }
  
  return rows
})

// Format value for display
const formatValue = (value) => {
  if (value === null || value === undefined) return '-'
  const num = parseFloat(value)
  if (!isNaN(num)) {
    return num.toLocaleString(undefined, { maximumFractionDigits: 2 })
  }
  return String(value)
}

// Get cell value
const getCellValue = (rowData, leafCol, measureKey) => {
  // Use fullPath for the column key
  const cell = rowData.cells.get(leafCol.fullPath)
  if (!cell) return '-'
  return formatValue(cell[measureKey])
}

// Handle column drill-down click
const handleColumnDrillDown = async (dimension, level, value) => {
  await store.drillDownColumn(dimension, level, value)
}

// Handle row drill-down click
const handleRowDrillDown = async (dimension, level, value) => {
  await store.drillDownRow(dimension, level, value)
}

// Handle sorting
const handleSort = (colPath, measure) => {
  if (sortConfig.value.colPath === colPath && sortConfig.value.measure === measure) {
    // Toggle direction if same column
    if (sortConfig.value.direction === 'desc') {
      sortConfig.value.direction = 'asc'
    } else {
      // Clear sort on third click
      sortConfig.value = { colPath: null, measure: null, direction: 'desc' }
    }
  } else {
    // New sort
    sortConfig.value = {
      colPath,
      measure,
      direction: 'desc'
    }
  }
}

// Check if column is being sorted
const isSorted = (colPath, measure) => {
  return sortConfig.value.colPath === colPath && sortConfig.value.measure === measure
}

const getSortDirection = (colPath, measure) => {
  if (isSorted(colPath, measure)) {
    return sortConfig.value.direction
  }
  return null
}
</script>

<template>
  <div class="pivot-table-container">
    <!-- Pivot Table Mode (when columns are defined) -->
    <template v-if="hasPivotColumns && pivotData">
      <div class="pivot-table-wrapper">
        <table class="pivot-table">
          <thead>
            <!-- Column dimension header rows -->
            <tr 
              v-for="(headerLevel, levelIdx) in columnHeaders" 
              :key="levelIdx"
              class="col-header-row"
            >
              <!-- Corner cells (row headers) - only on first row -->
              <th 
                v-if="levelIdx === 0"
                v-for="rowKey in rowKeys" 
                :key="rowKey.key"
                class="corner-cell"
                :rowspan="columnHeaders.length + (measureKeys.length > 1 ? 1 : 0)"
              >
                <span class="header-label">{{ rowKey.dimension }}</span>
                <span class="header-sublabel">{{ rowKey.level }}</span>
              </th>
              
              <!-- Column headers with colspan -->
              <th 
                v-for="col in headerLevel.values" 
                :key="col.fullPath"
                :colspan="col.colspan"
                :class="['col-header-cell', { 
                  'can-drill': col.canDrillDown, 
                  'is-expanded': col.isExpanded,
                  'level-0': levelIdx === 0,
                  'level-child': levelIdx > 0
                }]"
                @click="col.canDrillDown && handleColumnDrillDown(col.dimension, col.level, col.value)"
              >
                <span class="drill-indicator" v-if="col.canDrillDown">
                  {{ col.isExpanded ? '▼' : '▶' }}
                </span>
                <span class="header-value">{{ col.value }}</span>
              </th>
            </tr>
            
            <!-- Measure headers with sort buttons -->
            <tr v-if="measureKeys.length > 1" class="measure-header-row">
              <template v-for="leafCol in leafColumns" :key="leafCol.fullPath">
                <th 
                  v-for="measure in measureKeys" 
                  :key="`${leafCol.fullPath}-${measure}`"
                  :class="['measure-header-cell', { 'is-sorted': isSorted(leafCol.fullPath, measure) }]"
                >
                  <div class="measure-header-content">
                    <span class="measure-name">{{ measure }}</span>
                    <button 
                      class="sort-btn"
                      :class="{ 
                        'active': isSorted(leafCol.fullPath, measure),
                        'asc': getSortDirection(leafCol.fullPath, measure) === 'asc',
                        'desc': getSortDirection(leafCol.fullPath, measure) === 'desc'
                      }"
                      @click.stop="handleSort(leafCol.fullPath, measure)"
                      :title="isSorted(leafCol.fullPath, measure) 
                        ? (getSortDirection(leafCol.fullPath, measure) === 'desc' ? 'Sort Ascending' : 'Clear Sort')
                        : 'Sort Descending'"
                    >
                      <span class="sort-icon">
                        <svg v-if="!isSorted(leafCol.fullPath, measure)" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <path d="M7 15l5 5 5-5"/>
                          <path d="M7 9l5-5 5 5"/>
                        </svg>
                        <svg v-else-if="getSortDirection(leafCol.fullPath, measure) === 'desc'" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <path d="M7 13l5 5 5-5"/>
                        </svg>
                        <svg v-else width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <path d="M7 11l5-5 5 5"/>
                        </svg>
                      </span>
                    </button>
                  </div>
                </th>
              </template>
            </tr>
            
            <!-- Single measure with sort (when only 1 measure, add sort to leaf columns) -->
            <tr v-if="measureKeys.length === 1" class="measure-header-row single-measure">
              <template v-for="leafCol in leafColumns" :key="leafCol.fullPath">
                <th class="measure-header-cell" :class="{ 'is-sorted': isSorted(leafCol.fullPath, measureKeys[0]) }">
                  <div class="measure-header-content">
                    <span class="measure-name">{{ measureKeys[0] }}</span>
                    <button 
                      class="sort-btn"
                      :class="{ 
                        'active': isSorted(leafCol.fullPath, measureKeys[0]),
                        'asc': getSortDirection(leafCol.fullPath, measureKeys[0]) === 'asc',
                        'desc': getSortDirection(leafCol.fullPath, measureKeys[0]) === 'desc'
                      }"
                      @click.stop="handleSort(leafCol.fullPath, measureKeys[0])"
                    >
                      <span class="sort-icon">
                        <svg v-if="!isSorted(leafCol.fullPath, measureKeys[0])" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <path d="M7 15l5 5 5-5"/>
                          <path d="M7 9l5-5 5 5"/>
                        </svg>
                        <svg v-else-if="getSortDirection(leafCol.fullPath, measureKeys[0]) === 'desc'" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <path d="M7 13l5 5 5-5"/>
                        </svg>
                        <svg v-else width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <path d="M7 11l5-5 5 5"/>
                        </svg>
                      </span>
                    </button>
                  </div>
                </th>
              </template>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(rowData, rowIndex) in pivotRows" :key="rowData.rowKey">
              <!-- Row dimension values -->
              <td 
                v-for="rowKey in rowKeys" 
                :key="rowKey.key"
                :class="['row-header-cell', { 
                  'can-drill': rowData.rowDims[rowKey.key]?.canDrillDown,
                  'is-expanded': rowData.rowDims[rowKey.key]?.isExpanded 
                }]"
                @click="rowData.rowDims[rowKey.key]?.canDrillDown && handleRowDrillDown(
                  rowData.rowDims[rowKey.key].dimension,
                  rowData.rowDims[rowKey.key].level,
                  rowData.rowDims[rowKey.key].value
                )"
              >
                <span class="drill-indicator" v-if="rowData.rowDims[rowKey.key]?.canDrillDown">
                  {{ rowData.rowDims[rowKey.key]?.isExpanded ? '▼' : '▶' }}
                </span>
                <span class="cell-value">{{ rowData.rowDims[rowKey.key]?.value ?? '-' }}</span>
              </td>
              
              <!-- Data cells -->
              <template v-for="leafCol in leafColumns" :key="leafCol.fullPath">
                <td 
                  v-for="measure in measureKeys" 
                  :key="`${leafCol.fullPath}-${measure}`"
                  :class="['data-cell', { 'sorted-column': isSorted(leafCol.fullPath, measure) }]"
                >
                  {{ getCellValue(rowData, leafCol, measure) }}
                </td>
              </template>
            </tr>
          </tbody>
        </table>
      </div>
      
      <!-- Drill-down hint -->
      <div class="drill-hint">
        💡 Click on <span class="drill-indicator">▶</span> headers to drill down • Click sort buttons <span class="sort-icon-hint">↕</span> to sort by column
      </div>
    </template>
    
    <!-- Flat Table Mode (no column dimensions) -->
    <template v-else>
      <div class="flat-table-wrapper">
        <table class="flat-table">
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
    </template>
  </div>
</template>

<style scoped>
.pivot-table-container {
  overflow: hidden;
}

.pivot-table-wrapper,
.flat-table-wrapper {
  overflow: auto;
  max-height: 500px;
}

.pivot-table,
.flat-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.8125rem;
}

/* Corner cells */
.corner-cell {
  background: var(--bg-tertiary);
  color: var(--text-secondary);
  font-weight: 600;
  text-align: left;
  padding: var(--spacing-sm) var(--spacing-md);
  border: 1px solid var(--border-color);
  position: sticky;
  left: 0;
  z-index: 3;
  vertical-align: bottom;
}

.header-label {
  display: block;
  color: var(--accent-secondary);
  font-size: 0.75rem;
}

.header-sublabel {
  display: block;
  color: var(--text-muted);
  font-size: 0.6875rem;
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

/* Column Headers */
.col-header-cell {
  background: linear-gradient(180deg, var(--bg-tertiary), var(--bg-elevated));
  color: var(--text-primary);
  font-weight: 600;
  text-align: center;
  padding: var(--spacing-sm) var(--spacing-md);
  border: 1px solid var(--border-color);
  position: sticky;
  top: 0;
  z-index: 1;
  white-space: nowrap;
  transition: all var(--transition-fast);
}

/* First level (parent) headers - more prominent */
.col-header-cell.level-0 {
  background: linear-gradient(180deg, rgba(0, 212, 255, 0.15), var(--bg-tertiary));
  color: var(--accent-primary);
  font-size: 0.875rem;
  padding: var(--spacing-md);
  border-bottom: 2px solid var(--accent-primary);
}

/* Child level headers */
.col-header-cell.level-child {
  background: var(--bg-elevated);
  font-size: 0.8125rem;
}

.col-header-cell.can-drill {
  cursor: pointer;
}

.col-header-cell.can-drill:hover {
  background: linear-gradient(180deg, var(--bg-elevated), var(--bg-tertiary));
  box-shadow: inset 0 0 20px rgba(0, 212, 255, 0.1);
}

.col-header-cell.is-expanded {
  background: linear-gradient(180deg, rgba(0, 212, 255, 0.2), var(--bg-elevated));
  border-bottom-color: var(--accent-primary);
}

/* Row Headers */
.row-header-cell {
  background: var(--bg-elevated);
  color: var(--text-primary);
  font-weight: 500;
  text-align: left;
  padding: var(--spacing-sm) var(--spacing-md);
  border: 1px solid var(--border-color);
  position: sticky;
  left: 0;
  z-index: 2;
  white-space: nowrap;
  transition: all var(--transition-fast);
}

.row-header-cell.can-drill {
  cursor: pointer;
  color: var(--accent-secondary);
}

.row-header-cell.can-drill:hover {
  background: var(--bg-tertiary);
}

.row-header-cell.is-expanded {
  background: rgba(124, 58, 237, 0.1);
  border-left-color: var(--accent-secondary);
  border-left-width: 3px;
}

/* Drill indicator */
.drill-indicator {
  display: inline-block;
  width: 16px;
  font-size: 0.625rem;
  color: var(--accent-primary);
  margin-right: var(--spacing-xs);
  transition: transform var(--transition-fast);
}

.col-header-cell .drill-indicator {
  font-size: 0.5rem;
}

/* Measure headers */
.measure-header-cell {
  background: var(--bg-elevated);
  color: var(--accent-success);
  font-weight: 500;
  font-size: 0.6875rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  text-align: center;
  padding: var(--spacing-xs) var(--spacing-sm);
  border: 1px solid var(--border-color);
  position: sticky;
  z-index: 1;
}

.measure-header-cell.is-sorted {
  background: rgba(16, 185, 129, 0.1);
  border-bottom: 2px solid var(--accent-success);
}

.measure-header-content {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-xs);
}

.measure-name {
  flex-shrink: 0;
}

/* Sort button */
.sort-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  padding: 0;
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  cursor: pointer;
  transition: all var(--transition-fast);
  flex-shrink: 0;
}

.sort-btn:hover {
  background: var(--bg-tertiary);
  color: var(--text-primary);
  border-color: var(--border-color);
}

.sort-btn.active {
  background: var(--accent-success);
  color: white;
  border-color: var(--accent-success);
}

.sort-btn.active:hover {
  background: var(--accent-success);
  filter: brightness(1.1);
}

.sort-icon {
  display: flex;
  align-items: center;
  justify-content: center;
}

/* Single measure row - different styling */
.measure-header-row.single-measure .measure-header-cell {
  background: var(--bg-tertiary);
  font-size: 0.625rem;
  padding: var(--spacing-xs);
}

/* Data Cells */
.data-cell {
  text-align: right;
  padding: var(--spacing-sm) var(--spacing-md);
  border: 1px solid var(--border-color);
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  color: var(--text-primary);
  background: var(--bg-secondary);
  transition: background var(--transition-fast);
}

.data-cell:hover {
  background: var(--bg-tertiary);
}

.data-cell.sorted-column {
  background: rgba(16, 185, 129, 0.05);
}

/* Drill-down hint */
.drill-hint {
  padding: var(--spacing-sm) var(--spacing-md);
  background: var(--bg-tertiary);
  border-top: 1px solid var(--border-color);
  font-size: 0.75rem;
  color: var(--text-muted);
  text-align: center;
}

.drill-hint .drill-indicator {
  color: var(--accent-primary);
  margin: 0 var(--spacing-xs);
}

.sort-icon-hint {
  color: var(--accent-success);
  margin: 0 var(--spacing-xs);
}

/* Flat table specific */
.flat-table th {
  background: var(--bg-elevated);
  color: var(--accent-primary);
  font-weight: 500;
  text-transform: uppercase;
  font-size: 0.6875rem;
  letter-spacing: 0.05em;
  padding: var(--spacing-sm) var(--spacing-md);
  border-bottom: 2px solid var(--accent-primary);
  position: sticky;
  top: 0;
  text-align: left;
}

.flat-table td {
  padding: var(--spacing-sm) var(--spacing-md);
  border-bottom: 1px solid var(--border-color);
}

.flat-table tr:hover td {
  background: var(--bg-tertiary);
}

/* Alternating row colors */
.pivot-table tbody tr:nth-child(even) .row-header-cell {
  background: var(--bg-tertiary);
}

.pivot-table tbody tr:nth-child(even) .data-cell {
  background: rgba(0, 0, 0, 0.1);
}

.pivot-table tbody tr:nth-child(even) .data-cell.sorted-column {
  background: rgba(16, 185, 129, 0.08);
}

/* Header value styling */
.header-value {
  font-weight: 600;
}

.cell-value {
  font-weight: 500;
}
</style>
