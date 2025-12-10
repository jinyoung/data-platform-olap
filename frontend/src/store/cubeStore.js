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
  
  async function executePivotQuery() {
    if (!currentCube.value) {
      error.value = 'No cube selected'
      return
    }
    
    loading.value = true
    error.value = null
    
    try {
      const query = {
        cube_name: currentCube.value,
        ...pivotConfig.value
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
      const query = {
        cube_name: currentCube.value,
        ...pivotConfig.value
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
    executeNaturalQuery
  }
})

