import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import * as api from '../services/api'

export const useCubeStore = defineStore('cube', () => {
  // State
  const cubes = ref([])
  const currentCube = ref(null)
  const cubeMetadata = ref(null)
  const loading = ref(false)
  const error = ref(null)
  
  // Pivot configuration
  const pivotConfig = ref({
    rows: [],
    columns: [],
    measures: [],
    filters: []
  })
  
  // Drill-down state: tracks expanded items
  // Format: { 'dimension:level:value': true }
  const expandedItems = ref({})
  
  // Query results
  const queryResult = ref(null)
  const generatedSQL = ref('')
  
  // Computed
  const hasCubes = computed(() => cubes.value.length > 0)
  
  const dimensions = computed(() => {
    if (!cubeMetadata.value) return []
    return cubeMetadata.value.dimensions.map(dim => ({
      ...dim,
      type: 'dimension'
    }))
  })
  
  const measures = computed(() => {
    if (!cubeMetadata.value) return []
    return cubeMetadata.value.measures.map(m => ({
      ...m,
      type: 'measure'
    }))
  })
  
  // Get the next level in a dimension hierarchy
  function getNextLevel(dimensionName, currentLevel) {
    const dim = cubeMetadata.value?.dimensions.find(d => d.name === dimensionName)
    if (!dim || !dim.levels) return null
    
    const currentIndex = dim.levels.findIndex(l => l.name === currentLevel)
    if (currentIndex === -1 || currentIndex >= dim.levels.length - 1) {
      return null // No next level
    }
    
    return dim.levels[currentIndex + 1]
  }
  
  // Check if a level has children
  function hasNextLevel(dimensionName, currentLevel) {
    return getNextLevel(dimensionName, currentLevel) !== null
  }
  
  // Get hierarchy info for a dimension
  function getDimensionHierarchy(dimensionName) {
    const dim = cubeMetadata.value?.dimensions.find(d => d.name === dimensionName)
    return dim?.levels || []
  }
  
  // Actions
  async function uploadSchema(file) {
    loading.value = true
    error.value = null
    try {
      const metadata = await api.uploadSchema(file)
      await loadCubes()
      return metadata
    } catch (e) {
      error.value = e.response?.data?.detail || e.message
      throw e
    } finally {
      loading.value = false
    }
  }
  
  async function uploadSchemaText(xmlContent) {
    loading.value = true
    error.value = null
    try {
      const metadata = await api.uploadSchemaText(xmlContent)
      await loadCubes()
      return metadata
    } catch (e) {
      error.value = e.response?.data?.detail || e.message
      throw e
    } finally {
      loading.value = false
    }
  }
  
  async function loadCubes() {
    loading.value = true
    error.value = null
    try {
      const response = await api.getCubes()
      cubes.value = response.cubes
      
      // Auto-select first cube if available
      if (cubes.value.length > 0 && !currentCube.value) {
        await selectCube(cubes.value[0])
      }
    } catch (e) {
      error.value = e.response?.data?.detail || e.message
    } finally {
      loading.value = false
    }
  }
  
  async function selectCube(cubeName) {
    loading.value = true
    error.value = null
    try {
      const metadata = await api.getCubeMetadata(cubeName)
      currentCube.value = cubeName
      cubeMetadata.value = metadata
      
      // Reset pivot configuration
      resetPivotConfig()
    } catch (e) {
      error.value = e.response?.data?.detail || e.message
    } finally {
      loading.value = false
    }
  }
  
  function resetPivotConfig() {
    pivotConfig.value = {
      rows: [],
      columns: [],
      measures: [],
      filters: []
    }
    expandedItems.value = {}
    queryResult.value = null
    generatedSQL.value = ''
  }
  
  function addToRows(field) {
    if (!pivotConfig.value.rows.find(f => f.dimension === field.dimension && f.level === field.level)) {
      pivotConfig.value.rows.push({ ...field })
    }
  }
  
  function addToColumns(field) {
    if (!pivotConfig.value.columns.find(f => f.dimension === field.dimension && f.level === field.level)) {
      pivotConfig.value.columns.push({ ...field })
    }
  }
  
  function addMeasure(measure) {
    if (!pivotConfig.value.measures.find(m => m.name === measure.name)) {
      pivotConfig.value.measures.push({ name: measure.name })
    }
  }
  
  function removeFromRows(index) {
    pivotConfig.value.rows.splice(index, 1)
  }
  
  function removeFromColumns(index) {
    pivotConfig.value.columns.splice(index, 1)
  }
  
  function removeMeasure(index) {
    pivotConfig.value.measures.splice(index, 1)
  }
  
  function addFilter(filter) {
    pivotConfig.value.filters.push(filter)
  }
  
  function removeFilter(index) {
    pivotConfig.value.filters.splice(index, 1)
  }
  
  // Drill-down: expand a column header to show next level
  async function drillDownColumn(dimensionName, currentLevel, value) {
    const key = `col:${dimensionName}:${currentLevel}:${value}`
    
    if (expandedItems.value[key]) {
      // Collapse: remove the expansion
      delete expandedItems.value[key]
    } else {
      // Expand: mark as expanded
      expandedItems.value[key] = true
    }
    
    // Re-execute query with updated drill-down state
    await executePivotQueryWithDrillDown()
  }
  
  // Drill-down: expand a row header to show next level
  async function drillDownRow(dimensionName, currentLevel, value) {
    const key = `row:${dimensionName}:${currentLevel}:${value}`
    
    if (expandedItems.value[key]) {
      // Collapse
      delete expandedItems.value[key]
    } else {
      // Expand
      expandedItems.value[key] = true
    }
    
    await executePivotQueryWithDrillDown()
  }
  
  // Check if an item is expanded
  function isExpanded(type, dimensionName, level, value) {
    const key = `${type}:${dimensionName}:${level}:${value}`
    return !!expandedItems.value[key]
  }
  
  // Build query configuration with drill-downs applied
  function buildDrillDownConfig() {
    const config = {
      rows: [...pivotConfig.value.rows],
      columns: [...pivotConfig.value.columns],
      measures: [...pivotConfig.value.measures],
      filters: [...pivotConfig.value.filters]
    }
    
    // Add expanded levels to columns
    for (const key of Object.keys(expandedItems.value)) {
      const [type, dimName, level, value] = key.split(':')
      const nextLevel = getNextLevel(dimName, level)
      
      if (nextLevel) {
        if (type === 'col') {
          // Check if next level already in columns
          const exists = config.columns.find(
            c => c.dimension === dimName && c.level === nextLevel.name
          )
          if (!exists) {
            // Find position of current level and insert after
            const currentIdx = config.columns.findIndex(
              c => c.dimension === dimName && c.level === level
            )
            if (currentIdx !== -1) {
              config.columns.splice(currentIdx + 1, 0, {
                dimension: dimName,
                level: nextLevel.name
              })
            }
          }
        } else if (type === 'row') {
          const exists = config.rows.find(
            r => r.dimension === dimName && r.level === nextLevel.name
          )
          if (!exists) {
            const currentIdx = config.rows.findIndex(
              r => r.dimension === dimName && r.level === level
            )
            if (currentIdx !== -1) {
              config.rows.splice(currentIdx + 1, 0, {
                dimension: dimName,
                level: nextLevel.name
              })
            }
          }
        }
        
        // Add filter for the expanded value
        config.filters.push({
          dimension: dimName,
          level: level,
          operator: '=',
          values: [value]
        })
      }
    }
    
    return config
  }
  
  async function executePivotQuery() {
    expandedItems.value = {} // Reset drill-downs on fresh query
    await executePivotQueryWithDrillDown()
  }
  
  async function executePivotQueryWithDrillDown() {
    if (!currentCube.value) {
      error.value = 'No cube selected'
      return
    }
    
    loading.value = true
    error.value = null
    
    try {
      const config = buildDrillDownConfig()
      const query = {
        cube_name: currentCube.value,
        ...config
      }
      
      const result = await api.executePivotQuery(query)
      queryResult.value = result
      generatedSQL.value = result.sql
      
      if (result.error) {
        error.value = result.error
      }
    } catch (e) {
      error.value = e.response?.data?.detail || e.message
    } finally {
      loading.value = false
    }
  }
  
  async function previewSQL() {
    if (!currentCube.value) return ''
    
    try {
      const config = buildDrillDownConfig()
      const query = {
        cube_name: currentCube.value,
        ...config
      }
      
      const result = await api.previewPivotSQL(query)
      generatedSQL.value = result.sql
      return result.sql
    } catch (e) {
      error.value = e.response?.data?.detail || e.message
      return ''
    }
  }
  
  async function executeNaturalQuery(question) {
    if (!currentCube.value) {
      error.value = 'No cube selected'
      return
    }
    
    loading.value = true
    error.value = null
    
    try {
      const result = await api.executeNL2SQL(question, currentCube.value)
      queryResult.value = result
      generatedSQL.value = result.sql
      
      if (result.error) {
        error.value = result.error
      }
      
      return result
    } catch (e) {
      error.value = e.response?.data?.detail || e.message
      throw e
    } finally {
      loading.value = false
    }
  }
  
  return {
    // State
    cubes,
    currentCube,
    cubeMetadata,
    loading,
    error,
    pivotConfig,
    expandedItems,
    queryResult,
    generatedSQL,
    
    // Computed
    hasCubes,
    dimensions,
    measures,
    
    // Actions
    uploadSchema,
    uploadSchemaText,
    loadCubes,
    selectCube,
    resetPivotConfig,
    addToRows,
    addToColumns,
    addMeasure,
    removeFromRows,
    removeFromColumns,
    removeMeasure,
    addFilter,
    removeFilter,
    executePivotQuery,
    previewSQL,
    executeNaturalQuery,
    
    // Drill-down
    drillDownColumn,
    drillDownRow,
    isExpanded,
    hasNextLevel,
    getNextLevel,
    getDimensionHierarchy,
    buildDrillDownConfig
  }
})
