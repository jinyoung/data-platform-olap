<script setup>
import { computed } from 'vue'

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

// Check if we have column dimensions (need pivot transformation)
const hasPivotColumns = computed(() => {
  return props.pivotConfig?.columns?.length > 0
})

// Get column keys from pivot config
const rowKeys = computed(() => {
  return props.pivotConfig?.rows?.map(r => `${r.dimension}_${r.level}`.toLowerCase()) || []
})

const colKeys = computed(() => {
  return props.pivotConfig?.columns?.map(c => `${c.dimension}_${c.level}`.toLowerCase()) || []
})

const measureKeys = computed(() => {
  return props.pivotConfig?.measures?.map(m => m.name.toLowerCase()) || []
})

// Extract unique column values for pivot headers
const columnValues = computed(() => {
  if (!hasPivotColumns.value || !props.result?.rows) return []
  
  const values = new Set()
  const colKey = colKeys.value[0] // Support single column dimension for now
  
  props.result.rows.forEach(row => {
    const val = row[colKey]
    if (val !== undefined && val !== null) {
      values.add(val)
    }
  })
  
  return Array.from(values).sort()
})

// Build pivot data structure
const pivotData = computed(() => {
  if (!hasPivotColumns.value || !props.result?.rows) {
    return null
  }
  
  const colKey = colKeys.value[0]
  const data = new Map()
  
  props.result.rows.forEach(row => {
    // Build row key from all row dimensions
    const rowKeyValue = rowKeys.value.map(k => row[k]).join('|')
    const colValue = row[colKey]
    
    if (!data.has(rowKeyValue)) {
      // Store row dimension values
      const rowDimValues = {}
      rowKeys.value.forEach(k => {
        rowDimValues[k] = row[k]
      })
      data.set(rowKeyValue, {
        rowDims: rowDimValues,
        cells: new Map()
      })
    }
    
    // Store measure values for this cell
    const measures = {}
    measureKeys.value.forEach(m => {
      measures[m] = row[m]
    })
    data.get(rowKeyValue).cells.set(colValue, measures)
  })
  
  return data
})

// Get unique row entries for rendering
const pivotRows = computed(() => {
  if (!pivotData.value) return []
  return Array.from(pivotData.value.values())
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
const getCellValue = (rowData, colValue, measureKey) => {
  const cell = rowData.cells.get(colValue)
  if (!cell) return '-'
  return formatValue(cell[measureKey])
}

// Get row dimension label
const getRowLabel = (rowData, key) => {
  return rowData.rowDims[key] ?? '-'
}
</script>

<template>
  <div class="pivot-table-container">
    <!-- Pivot Table Mode (when columns are defined) -->
    <template v-if="hasPivotColumns && pivotData">
      <div class="pivot-table-wrapper">
        <table class="pivot-table">
          <thead>
            <!-- Column dimension header row -->
            <tr class="col-header-row">
              <th 
                v-for="rowKey in rowKeys" 
                :key="rowKey"
                class="row-header-cell corner-cell"
                :rowspan="measureKeys.length > 1 ? 2 : 1"
              >
                {{ rowKey.replace('_', ' › ') }}
              </th>
              <th 
                v-for="colValue in columnValues" 
                :key="colValue"
                :colspan="measureKeys.length"
                class="col-header-cell"
              >
                {{ colValue }}
              </th>
            </tr>
            <!-- Measure headers (if multiple measures) -->
            <tr v-if="measureKeys.length > 1" class="measure-header-row">
              <template v-for="colValue in columnValues" :key="colValue">
                <th 
                  v-for="measure in measureKeys" 
                  :key="`${colValue}-${measure}`"
                  class="measure-header-cell"
                >
                  {{ measure }}
                </th>
              </template>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(rowData, rowIndex) in pivotRows" :key="rowIndex">
              <!-- Row dimension values -->
              <td 
                v-for="rowKey in rowKeys" 
                :key="rowKey"
                class="row-header-cell"
              >
                {{ getRowLabel(rowData, rowKey) }}
              </td>
              <!-- Data cells -->
              <template v-for="colValue in columnValues" :key="colValue">
                <td 
                  v-for="measure in measureKeys" 
                  :key="`${colValue}-${measure}`"
                  class="data-cell"
                >
                  {{ getCellValue(rowData, colValue, measure) }}
                </td>
              </template>
            </tr>
          </tbody>
        </table>
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

/* Corner and Row Headers */
.corner-cell,
.row-header-cell {
  background: var(--bg-elevated);
  color: var(--accent-secondary);
  font-weight: 600;
  text-align: left;
  padding: var(--spacing-sm) var(--spacing-md);
  border: 1px solid var(--border-color);
  position: sticky;
  left: 0;
  z-index: 2;
  white-space: nowrap;
  text-transform: capitalize;
}

.corner-cell {
  z-index: 3;
  background: var(--bg-tertiary);
}

/* Column Headers */
.col-header-cell {
  background: linear-gradient(180deg, var(--bg-tertiary), var(--bg-elevated));
  color: var(--accent-primary);
  font-weight: 600;
  text-align: center;
  padding: var(--spacing-sm) var(--spacing-md);
  border: 1px solid var(--border-color);
  position: sticky;
  top: 0;
  z-index: 1;
  white-space: nowrap;
}

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
  top: 36px;
  z-index: 1;
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

/* Alternating row colors for better readability */
.pivot-table tbody tr:nth-child(even) .row-header-cell,
.pivot-table tbody tr:nth-child(even) .data-cell {
  background: rgba(0, 0, 0, 0.15);
}

.pivot-table tbody tr:nth-child(even) .row-header-cell {
  background: var(--bg-tertiary);
}
</style>

