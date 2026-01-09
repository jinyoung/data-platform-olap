<script setup>
import { ref, computed, watch, nextTick, onMounted } from 'vue'
import { useCubeStore } from '../store/cubeStore'
import * as api from '../services/api'
import mermaid from 'mermaid'

// API Gateway URL - 모든 마이크로서비스 요청의 단일 진입점
const API_GATEWAY_URL = import.meta.env.VITE_API_GATEWAY_URL ?? 'http://localhost:9000'
const ETL_API_BASE = `${API_GATEWAY_URL}/olap/api`
const TEXT2SQL_API_BASE = `${API_GATEWAY_URL}/text2sql`

// Initialize Mermaid
onMounted(async () => {
  mermaid.initialize({
    startOnLoad: false,
    theme: 'dark',
    themeVariables: {
      primaryColor: '#0d1929',
      primaryTextColor: '#e2e8f0',
      primaryBorderColor: '#00d4ff',
      lineColor: '#00d4ff',
      secondaryColor: '#1a2744',
      tertiaryColor: '#0f1d32',
      edgeLabelBackground: '#0d1929',
      clusterBkg: '#1a2744',
      clusterBorder: '#00d4ff'
    },
    er: {
      diagramPadding: 20,
      layoutDirection: 'TB',
      minEntityWidth: 100,
      minEntityHeight: 75,
      entityPadding: 15,
      useMaxWidth: true
    }
  })
  
  // Load catalog on mount
  await fetchCatalog()
})

const store = useCubeStore()
const activeTab = ref('etl') // 'visual', 'prompt', or 'etl'
const previewMode = ref('diagram') // 'xml' or 'diagram'
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const diagramContainer = ref(null)
const sqlPreviewRef = ref(null)
const resultDiagramContainer = ref(null)
const lineageDiagramContainer = ref(null)

// Visual Modeler State
const cubeName = ref('')
const factTable = ref('')
const dimensions = ref([])
const measures = ref([])

// ETL State
const catalog = ref({ tables: [], schemas: [], relationships: [] })
const etlDescription = ref('')
const aiSuggestion = ref(null)
const dwSchema = ref('dw')

// Fetch catalog from Neo4j
async function fetchCatalog() {
  try {
    const response = await fetch(`${ETL_API_BASE}/etl/catalog`)
    if (response.ok) {
      catalog.value = await response.json()
    }
  } catch (e) {
    console.error('Failed to fetch catalog:', e)
  }
}

// Get AI suggestion and populate visual modeler
async function getAISuggestion() {
  if (!etlDescription.value.trim()) {
    error.value = '큐브 설명을 입력해주세요'
    return
  }
  
  loading.value = true
  error.value = null
  aiSuggestion.value = null
  
  try {
    const response = await fetch(`${ETL_API_BASE}/etl/suggest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cube_description: etlDescription.value })
    })
    
    if (!response.ok) throw new Error('Failed to get AI suggestion')
    const data = await response.json()
    aiSuggestion.value = data.suggestion
    
    if (aiSuggestion.value && !aiSuggestion.value.error) {
      // Populate Visual Modeler from AI suggestion
      await populateFromAISuggestion(aiSuggestion.value)
      success.value = 'AI 추천이 완료되었습니다. 스타 스키마를 확인하고 수정하세요.'
      
      // Render result diagrams after values are set
      await nextTick()
      await renderResultDiagrams()
    }
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// Populate visual modeler from AI suggestion
async function populateFromAISuggestion(suggestion) {
  // Set cube name from description (first few words)
  if (!cubeName.value) {
    cubeName.value = etlDescription.value.split(/\s+/).slice(0, 2).join('').replace(/[^a-zA-Z0-9가-힣]/g, '') || 'AnalyticsCube'
  }
  
  // Set fact table
  const factSources = suggestion.fact_sources || []
  factTable.value = `${dwSchema.value}.fact_${cubeName.value.toLowerCase().replace(/[^a-z0-9]/g, '_')}`
  
  // Create dimensions from suggestion
  const dimSources = suggestion.dimension_sources || {}
  dimensions.value = Object.entries(dimSources).map(([dimName, sources], idx) => {
    const cleanName = dimName.replace('dim_', '').replace(/_/g, ' ')
    const titleName = cleanName.charAt(0).toUpperCase() + cleanName.slice(1)
    
    // Find mappings for this dimension
    const dimMappings = (suggestion.suggested_mappings || [])
      .filter(m => m.target && m.target.startsWith(dimName))
    
    const levels = dimMappings.length > 0 
      ? dimMappings.map((m, i) => ({
          id: Date.now() + i,
          name: (m.target.split('.').pop() || 'Name').replace(/_/g, ' '),
          column: m.target.split('.').pop() || 'name'
        }))
      : [{ id: Date.now(), name: 'Name', column: 'name' }]
    
    return {
      id: Date.now() + idx,
      name: titleName,
      table: `${dwSchema.value}.${dimName}`,
      foreignKey: `${dimName}_id`,
      levels
    }
  })
  
  // Create measures from mappings
  const measureMappings = (suggestion.suggested_mappings || [])
    .filter(m => m.transformation && (
      m.transformation.includes('SUM') || 
      m.transformation.includes('COUNT') || 
      m.transformation.includes('AVG')
    ))
  
  measures.value = measureMappings.length > 0
    ? measureMappings.map((m, idx) => {
        const colName = m.target.split('.').pop() || 'amount'
        const aggregator = m.transformation.includes('SUM') ? 'sum' 
          : m.transformation.includes('COUNT') ? 'count' 
          : m.transformation.includes('AVG') ? 'avg' : 'sum'
        
        return {
          id: Date.now() + idx,
          name: colName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
          column: colName,
          aggregator
        }
      })
    : [{ id: Date.now(), name: 'Total Amount', column: 'amount', aggregator: 'sum' }]
  
  // Switch to diagram view to show the star schema
  previewMode.value = 'diagram'
  
  // Save ETL config to backend
  try {
    const etlConfig = {
      cube_name: cubeName.value,
      fact_table: factTable.value,
      dimension_tables: Object.keys(dimSources),
      source_tables: [...(suggestion.fact_sources || []), ...Object.values(dimSources).flat()].filter(t => t && !t.startsWith('.')),
      mappings: (suggestion.suggested_mappings || []).map(m => ({
        source_table: m.source?.split('.')[0] || '',
        source_column: m.source?.split('.').pop() || '',
        target_table: m.target?.split('.')[0] || '',
        target_column: m.target?.split('.').pop() || '',
        transformation: m.transformation || ''
      })),
      dw_schema: dwSchema.value,
      sync_mode: suggestion.sync_strategy || 'full',
      incremental_column: suggestion.incremental_column || null
    }
    
    await fetch(`${ETL_API_BASE}/etl/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(etlConfig)
    })
    console.log('ETL config saved for cube:', cubeName.value)
  } catch (e) {
    console.error('Failed to save ETL config:', e)
  }
}

// Group tables by schema for catalog display
const tablesBySchema = computed(() => {
  const grouped = {}
  for (const table of catalog.value.tables || []) {
    const schema = table.schema || 'public'
    if (!grouped[schema]) grouped[schema] = []
    grouped[schema].push(table)
  }
  return grouped
})

// Prompt Generator State
const promptText = ref('')
const generatedXML = ref('')

// Table Generation State
const showTableGenerator = ref(false)
const uploadedCubeName = ref('')
const generatedSQL = ref('')
const sampleRowCount = ref(100)
const generatingTables = ref(false)
const executingSQL = ref(false)
const executionResult = ref(null)

// Add new dimension
const addDimension = () => {
  dimensions.value.push({
    id: Date.now(),
    name: '',
    table: '',
    foreignKey: '',
    levels: [{ id: Date.now(), name: '', column: '' }]
  })
}

// Remove dimension
const removeDimension = (index) => {
  dimensions.value.splice(index, 1)
}

// Add level to dimension
const addLevel = (dimIndex) => {
  dimensions.value[dimIndex].levels.push({
    id: Date.now(),
    name: '',
    column: ''
  })
}

// Remove level from dimension
const removeLevel = (dimIndex, levelIndex) => {
  dimensions.value[dimIndex].levels.splice(levelIndex, 1)
}

// Add new measure
const addMeasure = () => {
  measures.value.push({
    id: Date.now(),
    name: '',
    column: '',
    aggregator: 'sum'
  })
}

// Remove measure
const removeMeasure = (index) => {
  measures.value.splice(index, 1)
}

// Generate XML from visual model
const generateXMLFromModel = () => {
  if (!cubeName.value || !factTable.value) {
    error.value = 'Cube name and fact table are required'
    return ''
  }
  
  let xml = `<?xml version="1.0" encoding="UTF-8"?>
<Schema name="${cubeName.value}Schema">
  <Cube name="${cubeName.value}">
    <Table name="${factTable.value}"/>
`

  // Add dimensions
  dimensions.value.forEach(dim => {
    if (dim.name && dim.table) {
      xml += `
    <Dimension name="${dim.name}" foreignKey="${dim.foreignKey || dim.name.toLowerCase() + '_id'}">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="${dim.table}"/>
`
      dim.levels.forEach(level => {
        if (level.name && level.column) {
          xml += `        <Level name="${level.name}" column="${level.column}"/>\n`
        }
      })
      xml += `      </Hierarchy>
    </Dimension>
`
    }
  })

  // Add measures
  measures.value.forEach(measure => {
    if (measure.name && measure.column) {
      xml += `
    <Measure name="${measure.name}" column="${measure.column}" aggregator="${measure.aggregator}" formatString="#,###"/>
`
    }
  })

  xml += `  </Cube>
</Schema>`

  return xml
}

// Preview generated XML
const previewXML = computed(() => {
  if (activeTab.value === 'visual') {
    return generateXMLFromModel()
  }
  return generatedXML.value
})

// Generate Mermaid ER Diagram for Star Schema
// Sanitize name for Mermaid (remove dots and special characters)
function sanitizeMermaidName(name) {
  if (!name) return 'unknown'
  return name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^_+/, '').replace(/_+$/, '').replace(/_+/g, '_') || 'entity'
}

const generateMermaidDiagram = computed(() => {
  // For 'visual' and 'etl' tabs, use the current values directly
  // For 'xml' tab, extract from XML
  const useDirectValues = activeTab.value === 'visual' || activeTab.value === 'etl'
  
  const currentCubeName = useDirectValues ? cubeName.value : extractCubeNameFromXML(generatedXML.value)
  const currentFactTable = useDirectValues ? factTable.value : extractFactTableFromXML(generatedXML.value)
  const currentDimensions = useDirectValues ? dimensions.value : extractDimensionsFromXML(generatedXML.value)
  const currentMeasures = useDirectValues ? measures.value : extractMeasuresFromXML(generatedXML.value)
  
  if (!currentCubeName || !currentFactTable) {
    return ''
  }
  
  // Sanitize fact table name for Mermaid
  const safeFactTable = sanitizeMermaidName(currentFactTable)
  
  let diagram = `erDiagram\n`
  
  // Add fact table with measures
  diagram += `    ${safeFactTable} {\n`
  diagram += `        int id PK\n`
  currentDimensions.forEach(dim => {
    const fk = sanitizeMermaidName(dim.foreignKey || `${dim.name.toLowerCase()}_id`)
    diagram += `        int ${fk} FK\n`
  })
  currentMeasures.forEach(measure => {
    const colName = sanitizeMermaidName(measure.column || measure.name.toLowerCase())
    diagram += `        decimal ${colName}\n`
  })
  diagram += `    }\n\n`
  
  // Add dimension tables
  currentDimensions.forEach(dim => {
    if (dim.name && dim.table) {
      const safeDimTable = sanitizeMermaidName(dim.table)
      diagram += `    ${safeDimTable} {\n`
      diagram += `        int id PK\n`
      dim.levels.forEach(level => {
        if (level.column) {
          const safeCol = sanitizeMermaidName(level.column)
          diagram += `        varchar ${safeCol}\n`
        }
      })
      diagram += `    }\n\n`
    }
  })
  
  // Add relationships
  currentDimensions.forEach(dim => {
    if (dim.name && dim.table) {
      const safeDimTable = sanitizeMermaidName(dim.table)
      diagram += `    ${safeDimTable} ||--o{ ${safeFactTable} : has\n`
    }
  })
  
  return diagram
})

// Generate lineage diagram for ETL mappings - shows field level connections
const generateLineageDiagram = computed(() => {
  if (!aiSuggestion.value || !aiSuggestion.value.suggested_mappings) return ''
  
  const mappings = aiSuggestion.value.suggested_mappings
  const factSources = aiSuggestion.value.fact_sources || []
  
  let diagram = `flowchart LR\n`
  
  // Collect source tables and their fields
  const sourceTableFields = {}
  mappings.forEach(m => {
    if (m.source) {
      const parts = m.source.split('.')
      const table = parts[0]
      const field = parts.slice(1).join('.')
      if (!sourceTableFields[table]) sourceTableFields[table] = new Set()
      if (field) sourceTableFields[table].add(field)
    }
  })
  factSources.forEach(t => {
    if (!sourceTableFields[t]) sourceTableFields[t] = new Set()
  })
  
  // Collect target tables and their fields
  const targetTableFields = {}
  mappings.forEach(m => {
    if (m.target) {
      const parts = m.target.split('.')
      const table = parts[0]
      const field = parts.slice(1).join('.')
      if (!targetTableFields[table]) targetTableFields[table] = new Set()
      if (field) targetTableFields[table].add(field)
    }
  })
  
  // OLTP Subgraph
  diagram += `    subgraph OLTP["🗄️ OLTP 원본 테이블"]\n`
  diagram += `        direction TB\n`
  Object.entries(sourceTableFields).forEach(([table, fields]) => {
    const safeTable = sanitizeMermaidName(table)
    if (fields.size > 0) {
      diagram += `        ${safeTable}["📊 <b>${table}</b><br/>`
      Array.from(fields).slice(0, 4).forEach(f => {
        diagram += `• ${sanitizeMermaidName(f)}<br/>`
      })
      if (fields.size > 4) diagram += `... +${fields.size - 4}개`
      diagram += `"]\n`
    } else {
      diagram += `        ${safeTable}["📊 ${table}"]\n`
    }
  })
  diagram += `    end\n\n`
  
  // ETL Transform Box (single node)
  diagram += `    ETL_TRANSFORM{{"⚙️ ETL<br/>Transform & Load"}}\n\n`
  
  // OLAP Subgraph with star schema
  diagram += `    subgraph OLAP["⭐ OLAP 스타 스키마"]\n`
  diagram += `        direction TB\n`
  
  // Fact table prominently
  const factTableName = factTable.value || 'fact_table'
  const safeFactName = sanitizeMermaidName(factTableName)
  const factFields = targetTableFields[factTableName] || new Set()
  
  diagram += `        ${safeFactName}[["🎯 <b>${factTableName}</b><br/>`
  measures.value.slice(0, 3).forEach(m => {
    diagram += `▣ ${m.aggregator?.toUpperCase() || 'SUM'}(${sanitizeMermaidName(m.column)})<br/>`
  })
  if (measures.value.length > 3) diagram += `... +${measures.value.length - 3}개`
  diagram += `"]]\n`
  
  // Dimension tables
  dimensions.value.forEach(dim => {
    if (dim.table) {
      const safeDimTable = sanitizeMermaidName(dim.table)
      const levels = dim.levels || []
      diagram += `        ${safeDimTable}["📐 <b>${dim.name}</b><br/>`
      levels.slice(0, 2).forEach(l => {
        diagram += `○ ${sanitizeMermaidName(l.column || l.name)}<br/>`
      })
      if (levels.length > 2) diagram += `... +${levels.length - 2}개`
      diagram += `"]\n`
      diagram += `        ${safeDimTable} -.->|FK| ${safeFactName}\n`
    }
  })
  diagram += `    end\n\n`
  
  // Connect source tables to ETL
  Object.keys(sourceTableFields).forEach(table => {
    const safeTable = sanitizeMermaidName(table)
    diagram += `    ${safeTable} -->|extract| ETL_TRANSFORM\n`
  })
  
  // Connect ETL to OLAP
  diagram += `    ETL_TRANSFORM -->|load| ${safeFactName}\n`
  dimensions.value.forEach(dim => {
    if (dim.table) {
      const safeDimTable = sanitizeMermaidName(dim.table)
      diagram += `    ETL_TRANSFORM -->|load| ${safeDimTable}\n`
    }
  })
  
  // Styling
  diagram += `\n`
  diagram += `    style OLTP fill:#0d1f33,stroke:#3b82f6,color:#fff\n`
  diagram += `    style OLAP fill:#1a0d2e,stroke:#8b5cf6,color:#fff\n`
  diagram += `    style ETL_TRANSFORM fill:#0d2e24,stroke:#10b981,color:#fff\n`
  
  return diagram
})

// Render result diagrams after AI suggestion
async function renderResultDiagrams() {
  await nextTick()
  
  // Use unique IDs to avoid mermaid conflicts
  const starSchemaId = `resultStarSchema_${Date.now()}`
  const lineageId = `resultLineage_${Date.now() + 1}`
  
  // Render star schema
  if (resultDiagramContainer.value && generateMermaidDiagram.value) {
    try {
      console.log('Rendering star schema:', generateMermaidDiagram.value)
      const { svg } = await mermaid.render(starSchemaId, generateMermaidDiagram.value)
      resultDiagramContainer.value.innerHTML = svg
    } catch (e) {
      console.error('Result diagram error:', e)
      resultDiagramContainer.value.innerHTML = `<div class="diagram-error">스타 스키마 다이어그램 생성 실패: ${e.message}</div>`
    }
  } else {
    console.log('Star schema not rendered:', { 
      hasContainer: !!resultDiagramContainer.value, 
      hasDiagram: !!generateMermaidDiagram.value 
    })
  }
  
  // Render lineage diagram
  if (lineageDiagramContainer.value && generateLineageDiagram.value) {
    try {
      const { svg } = await mermaid.render(lineageId, generateLineageDiagram.value)
      lineageDiagramContainer.value.innerHTML = svg
    } catch (e) {
      console.error('Lineage diagram error:', e)
      lineageDiagramContainer.value.innerHTML = `<div class="diagram-error">리니지 다이어그램 생성 실패</div>`
    }
  }
}

// Helper functions to extract info from XML
function extractCubeNameFromXML(xml) {
  if (!xml) return ''
  const match = xml.match(/<Cube\s+name="([^"]+)"/)
  return match ? match[1] : ''
}

function extractFactTableFromXML(xml) {
  if (!xml) return ''
  const match = xml.match(/<Table\s+name="([^"]+)"/)
  return match ? match[1] : ''
}

function extractDimensionsFromXML(xml) {
  if (!xml) return []
  const dims = []
  const dimRegex = /<Dimension\s+name="([^"]+)"[^>]*foreignKey="([^"]+)"[^>]*>[\s\S]*?<Table\s+name="([^"]+)"[\s\S]*?(<Level[^>]+>[\s\S]*?)<\/Hierarchy>/g
  let match
  
  while ((match = dimRegex.exec(xml)) !== null) {
    const dim = {
      name: match[1],
      foreignKey: match[2],
      table: match[3],
      levels: []
    }
    
    const levelsStr = match[4]
    const levelRegex = /<Level\s+name="([^"]+)"\s+column="([^"]+)"/g
    let levelMatch
    while ((levelMatch = levelRegex.exec(levelsStr)) !== null) {
      dim.levels.push({ name: levelMatch[1], column: levelMatch[2] })
    }
    
    dims.push(dim)
  }
  
  return dims
}

function extractMeasuresFromXML(xml) {
  if (!xml) return []
  const measures = []
  const measureRegex = /<Measure\s+name="([^"]+)"\s+column="([^"]+)"\s+aggregator="([^"]+)"/g
  let match
  
  while ((match = measureRegex.exec(xml)) !== null) {
    measures.push({
      name: match[1],
      column: match[2],
      aggregator: match[3]
    })
  }
  
  return measures
}

// Render Mermaid diagram
async function renderMermaidDiagram() {
  if (!diagramContainer.value || !generateMermaidDiagram.value) return
  
  try {
    const { svg } = await mermaid.render('star-schema-diagram', generateMermaidDiagram.value)
    diagramContainer.value.innerHTML = svg
  } catch (e) {
    console.error('Mermaid render error:', e)
    diagramContainer.value.innerHTML = `<div class="diagram-error">Diagram rendering error: ${e.message}</div>`
  }
}

// Watch for changes and re-render diagram
watch([previewMode, generateMermaidDiagram], async ([mode]) => {
  if (mode === 'diagram') {
    await nextTick()
    await renderMermaidDiagram()
  }
}, { immediate: true })

// Auto-scroll SQL preview during streaming
watch(generatedSQL, async () => {
  if (generatingTables.value && sqlPreviewRef.value) {
    await nextTick()
    sqlPreviewRef.value.scrollTop = sqlPreviewRef.value.scrollHeight
  }
})

// Upload cube to server
const uploadCube = async () => {
  const xml = activeTab.value === 'visual' ? generateXMLFromModel() : generatedXML.value
  
  if (!xml) {
    error.value = 'No XML to upload'
    return
  }
  
  loading.value = true
  error.value = null
  success.value = null
  
  try {
    const result = await store.uploadSchemaText(xml)
    
    // Get the cube name from the result
    if (result.cubes && result.cubes.length > 0) {
      uploadedCubeName.value = result.cubes[0].name
      showTableGenerator.value = true
      success.value = `Cube "${uploadedCubeName.value}" uploaded successfully! Would you like to create database tables and sample data?`
    } else {
      success.value = 'Cube uploaded successfully!'
    }
    
  } catch (e) {
    error.value = e.response?.data?.detail || e.message || 'Failed to upload cube'
  } finally {
    loading.value = false
  }
}

// Generate table DDL and sample data with streaming
const generateTables = async () => {
  if (!uploadedCubeName.value) {
    error.value = 'No cube selected for table generation'
    return
  }
  
  generatingTables.value = true
  error.value = null
  generatedSQL.value = ''
  executionResult.value = null
  
  try {
    // Use streaming endpoint
    const response = await fetch(
      `${TEXT2SQL_API_BASE}/cube/${uploadedCubeName.value}/generate-tables-stream?sample_rows=${sampleRowCount.value}`,
      {
        method: 'GET',
        headers: { 'Accept': 'text/event-stream' }
      }
    )
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }
    
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    
    while (true) {
      const { done, value } = await reader.read()
      
      if (done) break
      
      buffer += decoder.decode(value, { stream: true })
      
      // Process complete SSE messages
      const lines = buffer.split('\n')
      buffer = lines.pop() || '' // Keep incomplete line in buffer
      
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const data = JSON.parse(line.slice(6))
            
            if (data.error) {
              error.value = data.error
              break
            }
            
            if (data.content) {
              // Append streaming content
              generatedSQL.value += data.content
            }
            
            if (data.done) {
              // Clean up markdown formatting if present
              if (generatedSQL.value.startsWith('```')) {
                const sqlLines = generatedSQL.value.split('\n')
                generatedSQL.value = sqlLines.slice(1, -1).join('\n')
              }
            }
          } catch (parseError) {
            // Skip invalid JSON
          }
        }
      }
    }
  } catch (e) {
    error.value = e.message || 'Failed to generate tables'
  } finally {
    generatingTables.value = false
  }
}

// Execute the generated SQL
const executeSQL = async () => {
  if (!generatedSQL.value) {
    error.value = 'No SQL to execute'
    return
  }
  
  executingSQL.value = true
  error.value = null
  executionResult.value = null
  
  try {
    const response = await fetch(`${TEXT2SQL_API_BASE}/cube/execute-sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: generatedSQL.value })
    })
    
    const data = await response.json()
    executionResult.value = data
    
    if (data.success) {
      success.value = `Tables created successfully! ${data.statements_executed} statements executed.`
    } else {
      error.value = data.error || 'Failed to execute SQL'
    }
  } catch (e) {
    error.value = e.message || 'Failed to execute SQL'
  } finally {
    executingSQL.value = false
  }
}

// Close table generator and reset
const closeTableGenerator = () => {
  showTableGenerator.value = false
  uploadedCubeName.value = ''
  generatedSQL.value = ''
  executionResult.value = null
  
  // Reset form
  if (activeTab.value === 'visual') {
    cubeName.value = ''
    factTable.value = ''
    dimensions.value = []
    measures.value = []
  } else {
    promptText.value = ''
    generatedXML.value = ''
  }
}

// Generate XML from prompt using AI
const generateFromPrompt = async () => {
  if (!promptText.value.trim()) {
    error.value = 'Please enter a description of the cube you want to create'
    return
  }
  
  loading.value = true
  error.value = null
  generatedXML.value = ''
  
  try {
    const response = await fetch(`${TEXT2SQL_API_BASE}/cube/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: promptText.value })
    })
    
    const data = await response.json()
    
    if (data.error) {
      error.value = data.error
    } else {
      generatedXML.value = data.xml
    }
  } catch (e) {
    error.value = e.message || 'Failed to generate cube'
  } finally {
    loading.value = false
  }
}

// Load sample template
const loadSampleTemplate = () => {
  cubeName.value = 'SalesCube'
  factTable.value = 'fact_sales'
  dimensions.value = [
    {
      id: 1,
      name: 'Time',
      table: 'dim_time',
      foreignKey: 'time_id',
      levels: [
        { id: 1, name: 'Year', column: 'year' },
        { id: 2, name: 'Quarter', column: 'quarter' },
        { id: 3, name: 'Month', column: 'month' }
      ]
    },
    {
      id: 2,
      name: 'Product',
      table: 'dim_product',
      foreignKey: 'product_id',
      levels: [
        { id: 1, name: 'Category', column: 'category' },
        { id: 2, name: 'ProductName', column: 'product_name' }
      ]
    }
  ]
  measures.value = [
    { id: 1, name: 'SalesAmount', column: 'sales_amount', aggregator: 'sum' },
    { id: 2, name: 'Quantity', column: 'quantity', aggregator: 'sum' }
  ]
}

// Sample prompts
const samplePrompts = [
  'Create an HR analytics cube with Employee dimension (Department > Team > Employee), Time dimension (Year > Quarter > Month), and measures for Headcount, Salary, and Turnover Rate',
  'Design a logistics cube tracking shipments with Dimensions: Origin (Country > City), Destination (Country > City), Carrier, Time (Year > Month). Measures: Shipment Count, Total Weight, Delivery Time, Cost',
  'Build a customer support cube with Ticket dimension (Category > Subcategory), Agent (Team > Agent), Priority, Time. Measures: Ticket Count, Resolution Time, Customer Satisfaction Score'
]

const useSamplePrompt = (prompt) => {
  promptText.value = prompt
}
</script>

<template>
  <div class="cube-modeler">
    <!-- Tab Navigation -->
    <div class="modeler-tabs">
      <button 
        :class="['tab-btn', { active: activeTab === 'etl' }]"
        @click="activeTab = 'etl'"
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M12 2v4m0 12v4M4.93 4.93l2.83 2.83m8.48 8.48l2.83 2.83M2 12h4m12 0h4M4.93 19.07l2.83-2.83m8.48-8.48l2.83-2.83"/>
        </svg>
        ETL 설계
      </button>
      <button 
        :class="['tab-btn', { active: activeTab === 'visual' }]"
        @click="activeTab = 'visual'"
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="3" y="3" width="7" height="7"/>
          <rect x="14" y="3" width="7" height="7"/>
          <rect x="14" y="14" width="7" height="7"/>
          <rect x="3" y="14" width="7" height="7"/>
        </svg>
        Visual Modeler
      </button>
      <button 
        :class="['tab-btn', { active: activeTab === 'prompt' }]"
        @click="activeTab = 'prompt'"
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M12 2a2 2 0 0 1 2 2c0 .74-.4 1.39-1 1.73V7h1a7 7 0 0 1 7 7h1a1 1 0 0 1 1 1v3a1 1 0 0 1-1 1h-1v1a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-1H2a1 1 0 0 1-1-1v-3a1 1 0 0 1 1-1h1a7 7 0 0 1 7-7h1V5.73c-.6-.34-1-.99-1-1.73a2 2 0 0 1 2-2z"/>
          <circle cx="7.5" cy="14.5" r="1.5"/>
          <circle cx="16.5" cy="14.5" r="1.5"/>
        </svg>
        AI Generator
      </button>
    </div>
    
    <!-- Messages -->
    <div v-if="error" class="message error">
      <span>⚠️ {{ error }}</span>
      <button @click="error = null">×</button>
    </div>
    <div v-if="success && !showTableGenerator" class="message success">
      <span>✅ {{ success }}</span>
      <button @click="success = null">×</button>
    </div>
    
    <!-- Table Generator Modal -->
    <div v-if="showTableGenerator" class="table-generator-overlay">
      <div class="table-generator-modal">
        <div class="modal-header">
          <h3>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <ellipse cx="12" cy="5" rx="9" ry="3"/>
              <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
              <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
            </svg>
            Create Database Tables
          </h3>
          <button class="close-btn" @click="closeTableGenerator">×</button>
        </div>
        
        <div class="modal-body">
          <div class="success-banner">
            <span class="success-icon">✅</span>
            <div>
              <strong>Cube "{{ uploadedCubeName }}" uploaded successfully!</strong>
              <p>Would you like to create the database tables and generate sample data?</p>
            </div>
          </div>
          
          <!-- Step 1: Generate SQL -->
          <div class="generator-step">
            <div class="step-header">
              <span class="step-number">1</span>
              <div>
                <h4>Generate Table DDL & Sample Data</h4>
                <p>AI will create PostgreSQL tables and realistic sample data</p>
              </div>
            </div>
            
            <div class="step-controls">
              <div class="row-count-input">
                <label>Sample rows:</label>
                <input type="number" v-model="sampleRowCount" min="10" max="1000" class="input" />
              </div>
              <button 
                class="btn btn-primary" 
                @click="generateTables" 
                :disabled="generatingTables"
              >
                <svg v-if="!generatingTables" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
                </svg>
                <span v-if="generatingTables" class="spinner"></span>
                {{ generatingTables ? 'Generating...' : 'Generate SQL' }}
              </button>
            </div>
            
            <!-- Generated SQL Preview -->
            <div v-if="generatedSQL || generatingTables" class="sql-preview-container">
              <div class="preview-label">
                <span>Generated SQL:</span>
                <span v-if="generatingTables" class="streaming-indicator">
                  <span class="streaming-dot"></span>
                  <span class="streaming-dot"></span>
                  <span class="streaming-dot"></span>
                  Streaming...
                </span>
              </div>
              <pre ref="sqlPreviewRef" class="sql-preview" :class="{ streaming: generatingTables }">{{ generatedSQL || '-- Generating SQL...' }}<span v-if="generatingTables" class="cursor-blink">|</span></pre>
            </div>
          </div>
          
          <!-- Step 2: Execute SQL -->
          <div v-if="generatedSQL" class="generator-step">
            <div class="step-header">
              <span class="step-number">2</span>
              <div>
                <h4>Execute SQL</h4>
                <p>Create tables and insert sample data into PostgreSQL</p>
              </div>
            </div>
            
            <div class="step-controls">
              <button 
                class="btn btn-success" 
                @click="executeSQL" 
                :disabled="executingSQL"
              >
                <svg v-if="!executingSQL" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <polygon points="5 3 19 12 5 21 5 3"/>
                </svg>
                <span v-if="executingSQL" class="spinner"></span>
                {{ executingSQL ? 'Executing...' : 'Execute SQL' }}
              </button>
            </div>
            
            <!-- Execution Result -->
            <div v-if="executionResult" class="execution-result" :class="{ success: executionResult.success, error: !executionResult.success }">
              <div v-if="executionResult.success" class="result-success">
                <span class="result-icon">✅</span>
                <div>
                  <strong>Tables created successfully!</strong>
                  <p>{{ executionResult.statements_executed }} statements executed, {{ executionResult.statements_failed }} failed</p>
                </div>
              </div>
              <div v-else class="result-error">
                <span class="result-icon">❌</span>
                <div>
                  <strong>Execution failed</strong>
                  <p>{{ executionResult.error }}</p>
                </div>
              </div>
            </div>
          </div>
        </div>
        
        <div class="modal-footer">
          <button class="btn btn-secondary" @click="closeTableGenerator">
            {{ executionResult?.success ? 'Done' : 'Skip' }}
          </button>
          <button v-if="executionResult?.success" class="btn btn-primary" @click="closeTableGenerator">
            Start Using Cube
          </button>
        </div>
      </div>
    </div>
    
    <div class="modeler-content">
      <!-- ETL Design Tab -->
      <div v-if="activeTab === 'etl'" class="etl-designer">
        <div class="etl-grid">
          <!-- Left: Source Catalog -->
          <div class="catalog-panel">
            <div class="panel-header">
              <h3>📚 원본 테이블 카탈로그</h3>
              <button class="btn-icon" @click="fetchCatalog" title="새로고침">🔄</button>
            </div>
            
            <div class="catalog-stats">
              <div class="stat">
                <span class="stat-value">{{ catalog.tables?.length || 0 }}</span>
                <span class="stat-label">테이블</span>
              </div>
            </div>
            
            <div class="table-list">
              <div v-for="(tables, schema) in tablesBySchema" :key="schema" class="schema-group">
                <div class="schema-header">
                  <span class="schema-icon">📁</span>
                  {{ schema }}
                  <span class="count">({{ tables.length }})</span>
                </div>
                <div class="table-items">
                  <div 
                    v-for="table in tables" 
                    :key="table.name"
                    class="table-item"
                    :title="table.description || '설명 없음'"
                  >
                    <span class="table-icon">📊</span>
                    <div class="table-info">
                      <span class="table-name">{{ table.name }}</span>
                      <span class="table-desc" v-if="table.description">{{ table.description }}</span>
                      <span class="table-columns">{{ table.columns?.length || 0 }} columns</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          
          <!-- Right: ETL Configuration -->
          <div class="etl-config-panel">
            <div class="panel-header">
              <h3>🤖 AI 기반 스타 스키마 설계</h3>
            </div>
            
            <div class="etl-form">
              <div class="form-group">
                <label>큐브 이름</label>
                <input v-model="cubeName" type="text" class="input" placeholder="예: OrderAnalytics" />
              </div>
              
              <div class="form-group">
                <label>DW 스키마</label>
                <input v-model="dwSchema" type="text" class="input" placeholder="dw" />
              </div>
              
              <div class="form-group">
                <label>분석 목표 설명</label>
                <textarea 
                  v-model="etlDescription" 
                  class="prompt-input"
                  placeholder="예: 주문 분석 큐브 - 제품별, 고객별, 카테고리별 총 주문량을 분석"
                  rows="4"
                ></textarea>
              </div>
              
              <button class="btn btn-primary generate-btn" @click="getAISuggestion" :disabled="loading || !etlDescription.trim()">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
                </svg>
                {{ loading ? 'AI 분석 중...' : 'AI 스타 스키마 생성' }}
              </button>
              
              <!-- AI Suggestion Summary - Full Results -->
              <div v-if="aiSuggestion && !aiSuggestion.error" class="ai-results-full">
                <!-- Two Column Layout for Diagrams -->
                <div class="diagrams-row">
                  <!-- Star Schema Diagram -->
                  <div class="result-card diagram-card">
                    <div class="result-card-header">
                      <span class="result-icon">⭐</span>
                      <h4>스타 스키마 설계</h4>
                    </div>
                    <div class="result-card-body">
                      <div ref="resultDiagramContainer" class="result-diagram-container"></div>
                    </div>
                  </div>
                  
                  <!-- Data Lineage / ETL Flow Diagram -->
                  <div class="result-card lineage-card">
                    <div class="result-card-header">
                      <span class="result-icon">🔗</span>
                      <h4>데이터 리니지 (ETL 흐름)</h4>
                    </div>
                    <div class="result-card-body">
                      <div ref="lineageDiagramContainer" class="lineage-diagram-container"></div>
                    </div>
                  </div>
                </div>
                
                <!-- ETL Strategy Details -->
                <div class="result-card etl-details-card">
                  <div class="result-card-header">
                    <span class="result-icon">📋</span>
                    <h4>ETL 파이프라인 상세</h4>
                  </div>
                  <div class="result-card-body">
                    <div class="etl-info-grid">
                      <div class="etl-info-item">
                        <span class="info-label">큐브 이름</span>
                        <span class="info-value highlight">{{ cubeName }}</span>
                      </div>
                      <div class="etl-info-item">
                        <span class="info-label">동기화 방식</span>
                        <span class="strategy-badge" :class="aiSuggestion.sync_strategy">
                          {{ aiSuggestion.sync_strategy === 'incremental' ? '🔄 증분' : '🔃 전체' }}
                        </span>
                      </div>
                      <div class="etl-info-item" v-if="aiSuggestion.incremental_column">
                        <span class="info-label">증분 기준</span>
                        <span class="info-value mono">{{ aiSuggestion.incremental_column }}</span>
                      </div>
                      <div class="etl-info-item">
                        <span class="info-label">팩트 테이블</span>
                        <span class="info-value mono">{{ factTable }}</span>
                      </div>
                    </div>
                    
                    <!-- Detailed Mappings Table -->
                    <div class="mappings-table-section">
                      <h5>📊 컬럼 매핑 상세 ({{ aiSuggestion.suggested_mappings?.length || 0 }}개)</h5>
                      <div class="mappings-table">
                        <div class="mappings-header">
                          <span class="col-source">원본 (OLTP)</span>
                          <span class="col-arrow"></span>
                          <span class="col-target">대상 (OLAP)</span>
                          <span class="col-transform">변환</span>
                        </div>
                        <div class="mappings-body">
                          <div v-for="(m, i) in aiSuggestion.suggested_mappings || []" :key="i" class="mapping-row">
                            <span class="col-source">
                              <span class="table-name">{{ m.source?.split('.')[0] }}</span>
                              <span class="column-name">.{{ m.source?.split('.').pop() }}</span>
                            </span>
                            <span class="col-arrow">→</span>
                            <span class="col-target">
                              <span class="table-name">{{ m.target?.split('.')[0] }}</span>
                              <span class="column-name">.{{ m.target?.split('.').pop() }}</span>
                            </span>
                            <span class="col-transform">{{ m.transformation || '-' }}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                    
                    <div class="ai-reasoning-box" v-if="aiSuggestion.reasoning">
                      <span class="reasoning-icon">💡</span>
                      <p>{{ aiSuggestion.reasoning }}</p>
                    </div>
                  </div>
                </div>
                
                <!-- Next Steps -->
                <div class="result-card guide-card">
                  <div class="result-card-header">
                    <span class="result-icon">📌</span>
                    <h4>다음 단계</h4>
                  </div>
                  <div class="result-card-body">
                    <div class="guide-steps-horizontal">
                      <div class="guide-step-h">
                        <span class="step-num">1</span>
                        <span class="step-text">Visual Modeler에서 스키마 확인</span>
                        <button class="btn btn-secondary btn-xs" @click="activeTab = 'visual'; previewMode = 'diagram'">→</button>
                      </div>
                      <div class="guide-step-h">
                        <span class="step-num">2</span>
                        <span class="step-text">Upload Cube로 큐브 생성</span>
                      </div>
                      <div class="guide-step-h highlight">
                        <span class="step-num">3</span>
                        <span class="step-text">피벗 분석에서 <span class="etl-btn-mini">🔄 ETL</span> 실행</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      
      <!-- Visual Modeler Tab -->
      <div v-if="activeTab === 'visual'" class="visual-modeler-new">
        <!-- Main Diagram Area (Left/Center) -->
        <div class="diagram-main-panel">
          <div class="diagram-header">
            <div class="diagram-title">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="3" y="3" width="7" height="7"/>
                <rect x="14" y="3" width="7" height="7"/>
                <rect x="14" y="14" width="7" height="7"/>
                <rect x="3" y="14" width="7" height="7"/>
              </svg>
              <h3>{{ cubeName || 'Star Schema' }}</h3>
            </div>
            <div class="diagram-actions">
              <div class="preview-toggle">
                <button 
                  :class="['toggle-btn', { active: previewMode === 'diagram' }]"
                  @click="previewMode = 'diagram'"
                >
                  ⭐ Diagram
                </button>
                <button 
                  :class="['toggle-btn', { active: previewMode === 'xml' }]"
                  @click="previewMode = 'xml'"
                >
                  &lt;/&gt; XML
                </button>
              </div>
              <button class="btn btn-primary" @click="uploadCube" :disabled="loading || !cubeName || !factTable">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                  <polyline points="17 8 12 3 7 8"/>
                  <line x1="12" y1="3" x2="12" y2="15"/>
                </svg>
                {{ loading ? 'Uploading...' : 'Upload Cube' }}
              </button>
            </div>
          </div>
          
          <!-- Diagram View -->
          <div v-if="previewMode === 'diagram'" class="diagram-canvas">
            <div ref="diagramContainer" class="mermaid-container-large">
              <div v-if="!generateMermaidDiagram" class="diagram-placeholder-large">
                <svg width="80" height="80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1">
                  <rect x="3" y="3" width="7" height="7"/>
                  <rect x="14" y="3" width="7" height="7"/>
                  <rect x="14" y="14" width="7" height="7"/>
                  <rect x="3" y="14" width="7" height="7"/>
                  <line x1="10" y1="6.5" x2="14" y2="6.5"/>
                  <line x1="6.5" y1="10" x2="6.5" y2="14"/>
                  <line x1="17.5" y1="10" x2="17.5" y2="14"/>
                </svg>
                <p>오른쪽에서 큐브를 설정하면 스타 스키마가 표시됩니다</p>
              </div>
            </div>
            <div v-if="generateMermaidDiagram" class="diagram-legend-bottom">
              <div class="legend-item"><span class="legend-icon fact">◼</span> Fact Table</div>
              <div class="legend-item"><span class="legend-icon dim">◼</span> Dimensions</div>
              <div class="legend-item"><span class="legend-line">──</span> FK Relations</div>
            </div>
          </div>
          
          <!-- XML View -->
          <pre v-else class="xml-preview-large">{{ previewXML || '<!-- 큐브를 설정하면 XML이 표시됩니다 -->' }}</pre>
        </div>
        
        <!-- Config Sidebar (Right) -->
        <div class="config-sidebar">
          <div class="sidebar-scroll">
            <!-- Cube Basic Info -->
            <div class="config-section">
              <div class="config-section-header">
                <span class="section-icon">🎲</span>
                <span>큐브 정보</span>
              </div>
              <div class="compact-form">
                <div class="form-row">
                  <label>Cube Name</label>
                  <input v-model="cubeName" type="text" class="input-sm" placeholder="SalesCube" />
                </div>
                <div class="form-row">
                  <label>Fact Table</label>
                  <input v-model="factTable" type="text" class="input-sm" placeholder="fact_sales" />
                </div>
              </div>
            </div>
            
            <!-- Dimensions -->
            <div class="config-section">
              <div class="config-section-header">
                <span class="section-icon">📊</span>
                <span>Dimensions ({{ dimensions.length }})</span>
                <button class="btn-add" @click="addDimension">+</button>
              </div>
              
              <div v-if="dimensions.length === 0" class="empty-hint">
                + 버튼을 눌러 차원을 추가하세요
              </div>
              
              <div class="dim-list">
                <div v-for="(dim, dimIndex) in dimensions" :key="dim.id" class="dim-card-compact">
                  <div class="dim-card-header">
                    <span class="dim-num">D{{ dimIndex + 1 }}</span>
                    <input v-model="dim.name" class="dim-name-input" placeholder="Name" />
                    <button class="btn-remove" @click="removeDimension(dimIndex)">×</button>
                  </div>
                  <div class="dim-card-body">
                    <input v-model="dim.table" class="input-xs" placeholder="Table" />
                    <input v-model="dim.foreignKey" class="input-xs" placeholder="FK" />
                  </div>
                  <div class="levels-compact">
                    <div class="levels-header-compact">
                      <span>Levels</span>
                      <button class="btn-add-sm" @click="addLevel(dimIndex)">+</button>
                    </div>
                    <div v-for="(level, levelIndex) in dim.levels" :key="level.id" class="level-row-compact">
                      <span class="level-num">L{{ levelIndex + 1 }}</span>
                      <input v-model="level.name" class="input-xs" placeholder="Level" />
                      <input v-model="level.column" class="input-xs" placeholder="Column" />
                      <button class="btn-remove-sm" @click="removeLevel(dimIndex, levelIndex)" :disabled="dim.levels.length <= 1">×</button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            
            <!-- Measures -->
            <div class="config-section">
              <div class="config-section-header">
                <span class="section-icon">📈</span>
                <span>Measures ({{ measures.length }})</span>
                <button class="btn-add" @click="addMeasure">+</button>
              </div>
              
              <div v-if="measures.length === 0" class="empty-hint">
                + 버튼을 눌러 측정값을 추가하세요
              </div>
              
              <div class="measure-list-compact">
                <div v-for="(measure, index) in measures" :key="measure.id" class="measure-row-compact">
                  <span class="measure-icon">Σ</span>
                  <input v-model="measure.name" class="input-xs" placeholder="Name" />
                  <input v-model="measure.column" class="input-xs" placeholder="Column" />
                  <select v-model="measure.aggregator" class="select-xs">
                    <option value="sum">SUM</option>
                    <option value="count">COUNT</option>
                    <option value="avg">AVG</option>
                    <option value="min">MIN</option>
                    <option value="max">MAX</option>
                  </select>
                  <button class="btn-remove-sm" @click="removeMeasure(index)">×</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      
      <!-- AI Prompt Tab -->
      <div v-if="activeTab === 'prompt'" class="prompt-generator">
        <div class="prompt-section">
          <h3>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
            </svg>
            Describe Your Cube
          </h3>
          <p class="hint">Describe the cube you want to create in natural language. Include dimensions, hierarchies, and measures.</p>
          
          <textarea 
            v-model="promptText"
            class="prompt-input"
            placeholder="Example: Create a sales analytics cube with dimensions for Time (Year > Quarter > Month), Product (Category > SubCategory > Product), and Region (Country > State > City). Include measures for Total Sales, Quantity Sold, Average Price, and Profit Margin."
            rows="6"
          ></textarea>
          
          <div class="sample-prompts">
            <span class="sample-label">Try these examples:</span>
            <button 
              v-for="(prompt, index) in samplePrompts" 
              :key="index"
              class="sample-btn"
              @click="useSamplePrompt(prompt)"
            >
              {{ prompt.substring(0, 50) }}...
            </button>
          </div>
          
          <button class="btn btn-primary generate-btn" @click="generateFromPrompt" :disabled="loading || !promptText.trim()">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
            </svg>
            {{ loading ? 'Generating...' : 'Generate with AI' }}
          </button>
        </div>
        
        <div class="generated-section" v-if="generatedXML">
          <div class="preview-header">
            <div class="preview-toggle">
              <button 
                :class="['toggle-btn', { active: previewMode === 'xml' }]"
                @click="previewMode = 'xml'"
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <polyline points="16 18 22 12 16 6"/>
                  <polyline points="8 6 2 12 8 18"/>
                </svg>
                XML
              </button>
              <button 
                :class="['toggle-btn', { active: previewMode === 'diagram' }]"
                @click="previewMode = 'diagram'"
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <rect x="3" y="3" width="7" height="7"/>
                  <rect x="14" y="3" width="7" height="7"/>
                  <rect x="14" y="14" width="7" height="7"/>
                  <rect x="3" y="14" width="7" height="7"/>
                </svg>
                Star Schema
              </button>
            </div>
            <button class="btn btn-primary" @click="uploadCube" :disabled="loading">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="17 8 12 3 7 8"/>
                <line x1="12" y1="3" x2="12" y2="15"/>
              </svg>
              {{ loading ? 'Uploading...' : 'Upload Cube' }}
            </button>
          </div>
          
          <!-- XML View -->
          <pre v-if="previewMode === 'xml'" class="xml-preview">{{ generatedXML }}</pre>
          
          <!-- Star Schema Diagram View -->
          <div v-else class="diagram-preview">
            <div ref="diagramContainer" class="mermaid-container"></div>
            <div v-if="generateMermaidDiagram" class="diagram-legend">
              <div class="legend-item">
                <span class="legend-icon fact">◼</span>
                <span>Fact Table</span>
              </div>
              <div class="legend-item">
                <span class="legend-icon dim">◼</span>
                <span>Dimension Tables</span>
              </div>
              <div class="legend-item">
                <span class="legend-line">──</span>
                <span>Foreign Key Relationships</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.cube-modeler {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
  height: 100%;
}

/* Tab Navigation */
.modeler-tabs {
  display: flex;
  gap: var(--spacing-xs);
  background: var(--bg-tertiary);
  padding: var(--spacing-xs);
  border-radius: var(--radius-lg);
  width: fit-content;
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

/* Messages */
.message {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-sm) var(--spacing-md);
  border-radius: var(--radius-md);
  font-size: 0.875rem;
}

.message.error {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid var(--accent-error);
  color: var(--accent-error);
}

.message.success {
  background: rgba(16, 185, 129, 0.1);
  border: 1px solid var(--accent-success);
  color: var(--accent-success);
}

.message button {
  background: none;
  border: none;
  color: inherit;
  font-size: 1.25rem;
  cursor: pointer;
  opacity: 0.7;
}

.message button:hover {
  opacity: 1;
}

/* Table Generator Modal */
.table-generator-overlay {
  position: fixed;
  inset: 0;
  background: rgba(10, 14, 23, 0.9);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  backdrop-filter: blur(4px);
  padding: var(--spacing-lg);
}

.table-generator-modal {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-xl);
  width: 100%;
  max-width: 800px;
  max-height: 90vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  box-shadow: 0 25px 50px rgba(0, 0, 0, 0.5);
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-lg);
  background: var(--bg-elevated);
  border-bottom: 1px solid var(--border-color);
}

.modal-header h3 {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  font-size: 1.125rem;
  color: var(--accent-primary);
}

.close-btn {
  width: 32px;
  height: 32px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  color: var(--text-muted);
  font-size: 1.25rem;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.close-btn:hover {
  background: var(--accent-error);
  border-color: var(--accent-error);
  color: white;
}

.modal-body {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-lg);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
}

.success-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-md);
  padding: var(--spacing-md);
  background: rgba(16, 185, 129, 0.1);
  border: 1px solid var(--accent-success);
  border-radius: var(--radius-md);
}

.success-icon {
  font-size: 1.5rem;
}

.success-banner strong {
  color: var(--accent-success);
}

.success-banner p {
  color: var(--text-secondary);
  font-size: 0.875rem;
  margin-top: var(--spacing-xs);
}

.generator-step {
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
}

.step-header {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-md);
  margin-bottom: var(--spacing-md);
}

.step-number {
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--accent-primary);
  color: white;
  border-radius: 50%;
  font-size: 0.875rem;
  font-weight: 600;
  flex-shrink: 0;
}

.step-header h4 {
  font-size: 1rem;
  color: var(--text-primary);
  margin-bottom: var(--spacing-xs);
}

.step-header p {
  font-size: 0.8125rem;
  color: var(--text-muted);
}

.step-controls {
  display: flex;
  align-items: center;
  gap: var(--spacing-md);
}

.row-count-input {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
}

.row-count-input label {
  font-size: 0.8125rem;
  color: var(--text-muted);
}

.row-count-input input {
  width: 80px;
}

.sql-preview-container {
  margin-top: var(--spacing-md);
}

.preview-label {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: var(--spacing-xs);
}

.streaming-indicator {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  color: var(--accent-primary);
  text-transform: none;
  letter-spacing: normal;
  font-weight: 500;
}

.streaming-dot {
  width: 4px;
  height: 4px;
  background: var(--accent-primary);
  border-radius: 50%;
  animation: streaming-pulse 1.4s ease-in-out infinite;
}

.streaming-dot:nth-child(1) { animation-delay: 0s; }
.streaming-dot:nth-child(2) { animation-delay: 0.2s; }
.streaming-dot:nth-child(3) { animation-delay: 0.4s; }

@keyframes streaming-pulse {
  0%, 80%, 100% {
    opacity: 0.3;
    transform: scale(0.8);
  }
  40% {
    opacity: 1;
    transform: scale(1.2);
  }
}

.sql-preview.streaming {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 1px var(--accent-primary), 0 0 20px rgba(0, 212, 255, 0.1);
}

.cursor-blink {
  animation: blink 0.8s step-end infinite;
  color: var(--accent-primary);
  font-weight: bold;
}

@keyframes blink {
  0%, 100% { opacity: 1; }
  50% { opacity: 0; }
}

.sql-preview {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  padding: var(--spacing-md);
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.6875rem;
  line-height: 1.5;
  color: var(--text-secondary);
  max-height: 200px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-all;
}

.btn-success {
  background: var(--accent-success);
  color: white;
  border: none;
}

.btn-success:hover:not(:disabled) {
  filter: brightness(1.1);
}

.execution-result {
  margin-top: var(--spacing-md);
  padding: var(--spacing-md);
  border-radius: var(--radius-md);
}

.execution-result.success {
  background: rgba(16, 185, 129, 0.1);
  border: 1px solid var(--accent-success);
}

.execution-result.error {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid var(--accent-error);
}

.result-success,
.result-error {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-sm);
}

.result-icon {
  font-size: 1.25rem;
}

.result-success strong {
  color: var(--accent-success);
}

.result-error strong {
  color: var(--accent-error);
}

.result-success p,
.result-error p {
  font-size: 0.8125rem;
  color: var(--text-secondary);
  margin-top: var(--spacing-xs);
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-sm);
  padding: var(--spacing-md) var(--spacing-lg);
  background: var(--bg-elevated);
  border-top: 1px solid var(--border-color);
}

.spinner {
  width: 16px;
  height: 16px;
  border: 2px solid transparent;
  border-top-color: currentColor;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

/* Modeler Content */
.modeler-content {
  flex: 1;
  overflow: hidden;
}

/* Visual Modeler - New Layout */
.visual-modeler-new {
  display: grid;
  grid-template-columns: 1fr 340px;
  gap: var(--spacing-lg);
  height: 100%;
}

.diagram-main-panel {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.diagram-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-md) var(--spacing-lg);
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-color);
}

.diagram-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
}

.diagram-title h3 {
  font-size: 1.125rem;
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.diagram-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-md);
}

.diagram-canvas {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.mermaid-container-large {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: var(--spacing-xl);
  overflow: auto;
  background: 
    radial-gradient(circle at center, rgba(0, 212, 255, 0.03) 0%, transparent 70%),
    var(--bg-primary);
}

.mermaid-container-large svg {
  max-width: 100%;
  height: auto;
}

.diagram-placeholder-large {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-lg);
  color: var(--text-muted);
  text-align: center;
}

.diagram-placeholder-large svg {
  opacity: 0.2;
}

.diagram-placeholder-large p {
  font-size: 0.9375rem;
}

.diagram-legend-bottom {
  display: flex;
  gap: var(--spacing-xl);
  justify-content: center;
  padding: var(--spacing-md);
  background: var(--bg-tertiary);
  border-top: 1px solid var(--border-color);
}

.xml-preview-large {
  flex: 1;
  margin: 0;
  padding: var(--spacing-lg);
  overflow: auto;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8125rem;
  line-height: 1.6;
  color: var(--text-secondary);
  background: var(--bg-primary);
  white-space: pre;
}

/* Config Sidebar */
.config-sidebar {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.sidebar-scroll {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-md);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-md);
}

.config-section {
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  padding: var(--spacing-md);
}

.config-section-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  margin-bottom: var(--spacing-sm);
  font-weight: 600;
  font-size: 0.875rem;
}

.section-icon {
  font-size: 1rem;
}

.config-section-header .btn-add {
  margin-left: auto;
}

.btn-add {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--accent-primary);
  color: white;
  border: none;
  border-radius: var(--radius-sm);
  font-size: 1rem;
  font-weight: 600;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.btn-add:hover {
  transform: scale(1.1);
}

.btn-add-sm {
  width: 18px;
  height: 18px;
  font-size: 0.75rem;
  background: var(--bg-elevated);
  color: var(--accent-primary);
  border: 1px solid var(--accent-primary);
  border-radius: 3px;
  cursor: pointer;
}

.compact-form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.form-row {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.form-row label {
  font-size: 0.6875rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
}

.input-sm {
  padding: var(--spacing-sm);
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.8125rem;
}

.input-xs {
  padding: 4px 8px;
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.75rem;
  flex: 1;
  min-width: 0;
}

.select-xs {
  padding: 4px 6px;
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.6875rem;
  width: 60px;
}

.empty-hint {
  padding: var(--spacing-sm);
  text-align: center;
  color: var(--text-muted);
  font-size: 0.75rem;
  font-style: italic;
}

/* Dimension Cards Compact */
.dim-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.dim-card-compact {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  padding: var(--spacing-sm);
}

.dim-card-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  margin-bottom: var(--spacing-xs);
}

.dim-num {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--accent-secondary);
  color: white;
  border-radius: 4px;
  font-size: 0.6875rem;
  font-weight: 700;
}

.dim-name-input {
  flex: 1;
  padding: 4px 8px;
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.8125rem;
  font-weight: 600;
}

.btn-remove {
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  color: var(--text-muted);
  font-size: 0.875rem;
  cursor: pointer;
}

.btn-remove:hover {
  background: var(--accent-error);
  border-color: var(--accent-error);
  color: white;
}

.btn-remove-sm {
  width: 16px;
  height: 16px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: none;
  color: var(--text-muted);
  font-size: 0.75rem;
  cursor: pointer;
  border-radius: 3px;
}

.btn-remove-sm:hover:not(:disabled) {
  background: var(--accent-error);
  color: white;
}

.btn-remove-sm:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

.dim-card-body {
  display: flex;
  gap: var(--spacing-xs);
  margin-bottom: var(--spacing-xs);
}

.levels-compact {
  background: var(--bg-tertiary);
  border-radius: 4px;
  padding: var(--spacing-xs);
}

.levels-header-compact {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 0.6875rem;
  color: var(--text-muted);
  margin-bottom: var(--spacing-xs);
}

.level-row-compact {
  display: flex;
  align-items: center;
  gap: 4px;
  margin-bottom: 4px;
}

.level-row-compact:last-child {
  margin-bottom: 0;
}

.level-num {
  width: 18px;
  height: 18px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-elevated);
  color: var(--accent-primary);
  border-radius: 3px;
  font-size: 0.625rem;
  font-weight: 600;
}

/* Measure List Compact */
.measure-list-compact {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-xs);
}

.measure-row-compact {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: var(--spacing-xs);
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
}

.measure-icon {
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--accent-success);
  color: white;
  border-radius: 4px;
  font-size: 0.75rem;
  font-weight: 700;
}

/* Legacy Visual Modeler (keep for compatibility) */
.visual-modeler {
  display: grid;
  grid-template-columns: 1fr 400px;
  gap: var(--spacing-lg);
  height: 100%;
}

.modeler-form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
  overflow-y: auto;
  padding-right: var(--spacing-sm);
}

.form-section {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-md);
  padding-bottom: var(--spacing-md);
  border-bottom: 1px solid var(--border-color);
}

.section-header h3 {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  font-size: 1rem;
  color: var(--accent-primary);
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: var(--spacing-md);
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-xs);
}

.form-group label {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
}

/* Dimension Card */
.dimension-card {
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  padding: var(--spacing-md);
  margin-top: var(--spacing-md);
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-md);
}

.dim-badge {
  background: var(--accent-secondary);
  color: white;
  padding: var(--spacing-xs) var(--spacing-sm);
  border-radius: var(--radius-sm);
  font-size: 0.6875rem;
  font-weight: 600;
  text-transform: uppercase;
}

.remove-btn {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.remove-btn:hover {
  background: var(--accent-error);
  border-color: var(--accent-error);
  color: white;
}

/* Levels */
.levels-section {
  margin-top: var(--spacing-md);
  padding-top: var(--spacing-md);
  border-top: 1px dashed var(--border-color);
}

.levels-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-sm);
}

.levels-label {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
}

.btn-sm {
  padding: var(--spacing-xs) var(--spacing-sm);
  font-size: 0.75rem;
}

.levels-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-xs);
}

.level-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
}

.level-number {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-elevated);
  border-radius: var(--radius-sm);
  font-size: 0.6875rem;
  color: var(--accent-primary);
  font-weight: 600;
}

.remove-btn-sm {
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: none;
  color: var(--text-muted);
  cursor: pointer;
  border-radius: var(--radius-sm);
  transition: all var(--transition-fast);
}

.remove-btn-sm:hover:not(:disabled) {
  background: var(--accent-error);
  color: white;
}

.remove-btn-sm:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

/* Measures */
.measures-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
  margin-top: var(--spacing-md);
}

.measure-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
}

.measure-badge {
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--accent-success);
  color: white;
  border-radius: var(--radius-sm);
  font-weight: 600;
}

.select {
  padding: var(--spacing-sm);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.875rem;
  min-width: 120px;
}

/* Empty placeholder */
.empty-placeholder {
  padding: var(--spacing-lg);
  text-align: center;
  color: var(--text-muted);
  font-size: 0.875rem;
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
  border: 2px dashed var(--border-color);
  margin-top: var(--spacing-md);
}

/* XML Preview Panel */
.xml-preview-panel {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.preview-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-md);
  background: var(--bg-elevated);
  border-bottom: 1px solid var(--border-color);
}

.preview-header h4 {
  font-size: 0.875rem;
  color: var(--text-secondary);
}

.xml-preview-panel .xml-preview {
  flex: 1;
  padding: var(--spacing-md);
  margin: 0;
  overflow: auto;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  line-height: 1.6;
  color: var(--text-secondary);
  background: var(--bg-primary);
  max-height: none;
  white-space: pre;
  word-break: normal;
}

/* Prompt Generator */
.prompt-generator {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
  max-width: 900px;
}

.prompt-section {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-xl);
}

.prompt-section h3 {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  font-size: 1.125rem;
  color: var(--accent-primary);
  margin-bottom: var(--spacing-sm);
}

.hint {
  color: var(--text-muted);
  font-size: 0.875rem;
  margin-bottom: var(--spacing-md);
}

.prompt-input {
  width: 100%;
  padding: var(--spacing-md);
  background: var(--bg-tertiary);
  border: 2px solid var(--border-color);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.9375rem;
  line-height: 1.6;
  resize: vertical;
  transition: all var(--transition-fast);
}

.prompt-input:focus {
  outline: none;
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(0, 212, 255, 0.1);
}

.prompt-input::placeholder {
  color: var(--text-muted);
}

.sample-prompts {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-sm);
  align-items: center;
  margin: var(--spacing-md) 0;
}

.sample-label {
  font-size: 0.75rem;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.sample-btn {
  padding: var(--spacing-xs) var(--spacing-sm);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  font-family: inherit;
  font-size: 0.75rem;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.sample-btn:hover {
  background: var(--bg-elevated);
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

.generate-btn {
  width: 100%;
  padding: var(--spacing-md);
  font-size: 1rem;
}

.generated-section {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  overflow: hidden;
}

.generated-section .xml-preview {
  max-height: 400px;
}

/* Preview Toggle */
.preview-toggle {
  display: flex;
  gap: 2px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
  padding: 2px;
}

.toggle-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  padding: var(--spacing-xs) var(--spacing-sm);
  background: transparent;
  border: none;
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  font-family: inherit;
  font-size: 0.75rem;
  font-weight: 500;
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

/* Diagram Preview */
.diagram-preview {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  background: var(--bg-primary);
}

.mermaid-container {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: var(--spacing-lg);
  overflow: auto;
  min-height: 300px;
}

.mermaid-container svg {
  max-width: 100%;
  height: auto;
}

.diagram-placeholder {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-md);
  color: var(--text-muted);
  text-align: center;
}

.diagram-placeholder svg {
  opacity: 0.3;
}

.diagram-placeholder p {
  font-size: 0.875rem;
}

.diagram-error {
  padding: var(--spacing-md);
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid var(--accent-error);
  border-radius: var(--radius-md);
  color: var(--accent-error);
  font-size: 0.8125rem;
}

.diagram-legend {
  display: flex;
  gap: var(--spacing-lg);
  padding: var(--spacing-md);
  background: var(--bg-elevated);
  border-top: 1px solid var(--border-color);
  justify-content: center;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  font-size: 0.75rem;
  color: var(--text-muted);
}

.legend-icon {
  font-size: 0.875rem;
}

.legend-icon.fact {
  color: var(--accent-primary);
}

.legend-icon.dim {
  color: var(--accent-secondary);
}

.legend-line {
  color: var(--accent-primary);
  font-size: 0.75rem;
}

/* ETL Designer Styles */
.etl-designer {
  height: 100%;
}

.etl-grid {
  display: grid;
  grid-template-columns: 320px 1fr;
  gap: var(--spacing-lg);
  height: 100%;
}

.catalog-panel,
.etl-config-panel {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-md) var(--spacing-lg);
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-color);
}

.panel-header h3 {
  font-size: 1rem;
  font-weight: 600;
}

.btn-icon {
  background: none;
  border: none;
  font-size: 1.25rem;
  cursor: pointer;
  padding: var(--spacing-xs);
  opacity: 0.7;
  transition: opacity var(--transition-fast);
}

.btn-icon:hover {
  opacity: 1;
}

.catalog-stats {
  display: flex;
  gap: var(--spacing-lg);
  padding: var(--spacing-md) var(--spacing-lg);
  border-bottom: 1px solid var(--border-color);
}

.stat {
  display: flex;
  flex-direction: column;
  align-items: center;
}

.stat-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--accent-primary);
}

.stat-label {
  font-size: 0.75rem;
  color: var(--text-muted);
}

.table-list {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-md);
}

.schema-group {
  margin-bottom: var(--spacing-md);
}

.schema-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm);
  font-weight: 600;
  color: var(--text-secondary);
  font-size: 0.875rem;
}

.schema-header .count {
  color: var(--text-muted);
  font-weight: normal;
}

.table-items {
  margin-left: var(--spacing-lg);
}

.table-item {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm);
  border-radius: var(--radius-md);
  cursor: default;
  transition: background var(--transition-fast);
}

.table-item:hover {
  background: var(--bg-elevated);
}

.table-item .table-icon {
  margin-top: 2px;
}

.table-info {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-width: 0;
}

.table-name {
  font-size: 0.875rem;
  font-weight: 500;
}

.table-desc {
  font-size: 0.75rem;
  color: var(--text-muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 200px;
}

.table-columns {
  font-size: 0.7rem;
  color: var(--text-muted);
  opacity: 0.7;
}

.etl-form {
  padding: var(--spacing-lg);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
}

.generate-btn {
  width: 100%;
  padding: var(--spacing-md);
  font-size: 1rem;
}

.ai-summary {
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
}

.ai-summary h4 {
  font-size: 0.875rem;
  color: var(--accent-primary);
  margin-bottom: var(--spacing-md);
}

.summary-section {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  margin-bottom: var(--spacing-sm);
}

.summary-label {
  font-size: 0.8125rem;
  color: var(--text-muted);
}

.strategy-badge {
  display: inline-block;
  padding: var(--spacing-xs) var(--spacing-sm);
  background: var(--bg-elevated);
  border-radius: var(--radius-sm);
  font-size: 0.75rem;
  font-weight: 500;
}

.strategy-badge.incremental {
  background: rgba(16, 185, 129, 0.1);
  color: var(--accent-success);
}

.source-tags {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-xs);
}

.source-tag {
  background: var(--bg-elevated);
  padding: 2px 8px;
  border-radius: var(--radius-sm);
  font-size: 0.75rem;
  font-family: monospace;
  color: var(--accent-primary);
}

.dim-count {
  color: var(--accent-secondary);
  font-weight: 600;
}

.ai-reasoning {
  font-size: 0.8125rem;
  color: var(--text-secondary);
  line-height: 1.5;
  margin-top: var(--spacing-md);
  padding: var(--spacing-sm);
  background: var(--bg-primary);
  border-radius: var(--radius-sm);
}

.next-step-hint {
  margin-top: var(--spacing-md);
  padding: var(--spacing-md);
  background: rgba(0, 212, 255, 0.1);
  border: 1px solid var(--accent-primary);
  border-radius: var(--radius-md);
  font-size: 0.875rem;
  color: var(--accent-primary);
}

.btn-link {
  background: none;
  border: none;
  color: var(--accent-primary);
  font-family: inherit;
  font-size: inherit;
  font-weight: 600;
  cursor: pointer;
  text-decoration: underline;
}

.btn-link:hover {
  color: var(--accent-secondary);
}

/* AI Results Cards */
.ai-results {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-md);
  max-height: 500px;
  overflow-y: auto;
}

.result-card {
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  overflow: hidden;
}

.result-card.etl-card {
  border-color: var(--accent-primary);
  border-width: 1px;
}

.result-card.guide-card {
  border-color: var(--accent-success);
}

.result-card-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-md);
  background: var(--bg-elevated);
  border-bottom: 1px solid var(--border-color);
}

.result-card-header h4 {
  font-size: 0.875rem;
  font-weight: 600;
  margin: 0;
}

.result-icon {
  font-size: 1rem;
}

.result-card-body {
  padding: var(--spacing-md);
}

.result-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  margin-bottom: var(--spacing-xs);
}

.result-label {
  font-size: 0.75rem;
  color: var(--text-muted);
  min-width: 80px;
}

.result-value {
  font-size: 0.8125rem;
  color: var(--text-primary);
}

.result-value.highlight {
  color: var(--accent-primary);
  font-weight: 600;
}

.result-value.mono {
  font-family: monospace;
  background: var(--bg-primary);
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 0.75rem;
}

.dim-tags,
.measure-tags,
.source-tags-large {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-xs);
  margin: var(--spacing-xs) 0 var(--spacing-sm) 0;
}

.dim-tag {
  background: rgba(124, 58, 237, 0.2);
  color: var(--accent-secondary);
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.6875rem;
  font-weight: 500;
}

.measure-tag {
  background: rgba(16, 185, 129, 0.2);
  color: var(--accent-success);
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.6875rem;
  font-family: monospace;
}

.source-tags-large .source-tag {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.75rem;
}

.etl-mappings {
  margin-top: var(--spacing-md);
  padding-top: var(--spacing-md);
  border-top: 1px dashed var(--border-color);
}

.mapping-header {
  font-size: 0.75rem;
  color: var(--text-muted);
  margin-bottom: var(--spacing-xs);
}

.mapping-list {
  background: var(--bg-primary);
  border-radius: var(--radius-sm);
  padding: var(--spacing-sm);
}

.mapping-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: 2px 0;
  font-size: 0.6875rem;
}

.mapping-from {
  color: var(--text-secondary);
  font-family: monospace;
}

.mapping-arrow {
  color: var(--accent-primary);
}

.mapping-to {
  color: var(--accent-success);
  font-family: monospace;
}

.mapping-more {
  text-align: center;
  padding: var(--spacing-xs);
  color: var(--text-muted);
  font-size: 0.6875rem;
  font-style: italic;
}

.ai-reasoning-box {
  display: flex;
  gap: var(--spacing-sm);
  margin-top: var(--spacing-md);
  padding: var(--spacing-md);
  background: rgba(0, 212, 255, 0.05);
  border: 1px solid rgba(0, 212, 255, 0.2);
  border-radius: var(--radius-md);
}

.reasoning-icon {
  font-size: 1.25rem;
  flex-shrink: 0;
}

.ai-reasoning-box p {
  font-size: 0.8125rem;
  color: var(--text-secondary);
  line-height: 1.5;
  margin: 0;
}

/* Guide Steps */
.guide-steps {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-md);
}

.guide-step {
  display: flex;
  gap: var(--spacing-md);
}

.step-num {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-elevated);
  color: var(--text-muted);
  border-radius: 50%;
  font-size: 0.75rem;
  font-weight: 600;
  flex-shrink: 0;
}

.guide-step.completed .step-num {
  background: var(--accent-success);
  color: white;
}

.step-content {
  flex: 1;
}

.step-content strong {
  font-size: 0.875rem;
  display: block;
  margin-bottom: 2px;
}

.step-content p {
  font-size: 0.75rem;
  color: var(--text-muted);
  margin: 0 0 var(--spacing-xs) 0;
  line-height: 1.4;
}

.btn-sm {
  padding: var(--spacing-xs) var(--spacing-sm);
  font-size: 0.75rem;
}

.etl-btn-preview {
  display: inline-block;
  background: var(--accent-primary);
  color: white;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.6875rem;
  font-weight: 500;
}

/* Full AI Results View */
.ai-results-full {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
}

.diagrams-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-lg);
}

@media (max-width: 1400px) {
  .diagrams-row {
    grid-template-columns: 1fr;
  }
}

.result-card.diagram-card,
.result-card.lineage-card {
  border-color: rgba(139, 92, 246, 0.3);
}

.result-card.etl-details-card {
  border-color: rgba(0, 212, 255, 0.3);
}

.result-diagram-container,
.lineage-diagram-container {
  min-height: 280px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: 
    radial-gradient(ellipse at 30% 20%, rgba(139, 92, 246, 0.08) 0%, transparent 50%),
    radial-gradient(ellipse at 70% 80%, rgba(0, 212, 255, 0.08) 0%, transparent 50%),
    var(--bg-primary);
  border-radius: var(--radius-md);
  overflow: auto;
  padding: var(--spacing-md);
}

.result-diagram-container :deep(svg),
.lineage-diagram-container :deep(svg) {
  max-width: 100%;
  height: auto;
}

.diagram-error {
  color: var(--text-muted);
  font-size: 0.875rem;
  text-align: center;
  padding: var(--spacing-lg);
}

/* ETL Info Grid */
.etl-info-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: var(--spacing-md);
  margin-bottom: var(--spacing-lg);
}

.etl-info-item {
  background: var(--bg-primary);
  padding: var(--spacing-md);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-color);
}

.info-label {
  display: block;
  font-size: 0.6875rem;
  color: var(--text-muted);
  margin-bottom: var(--spacing-xs);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.info-value {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--text-primary);
}

.info-value.highlight {
  color: var(--accent-primary);
}

.info-value.mono {
  font-family: monospace;
  font-size: 0.75rem;
}

/* Mappings Table */
.mappings-table-section {
  border-top: 1px solid var(--border-color);
  padding-top: var(--spacing-lg);
}

.mappings-table-section h5 {
  font-size: 0.875rem;
  margin: 0 0 var(--spacing-md) 0;
  color: var(--text-primary);
}

.mappings-table {
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.mappings-header {
  display: grid;
  grid-template-columns: 1fr 40px 1fr 1fr;
  background: var(--bg-elevated);
  padding: var(--spacing-sm) var(--spacing-md);
  border-bottom: 1px solid var(--border-color);
  font-size: 0.6875rem;
  font-weight: 600;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.mappings-body {
  max-height: 300px;
  overflow-y: auto;
}

.mapping-row {
  display: grid;
  grid-template-columns: 1fr 40px 1fr 1fr;
  padding: var(--spacing-sm) var(--spacing-md);
  border-bottom: 1px solid var(--border-color);
  font-size: 0.75rem;
  align-items: center;
  transition: background 0.15s;
}

.mapping-row:last-child {
  border-bottom: none;
}

.mapping-row:hover {
  background: var(--bg-tertiary);
}

.col-source {
  font-family: monospace;
}

.col-source .table-name {
  color: #3b82f6;
}

.col-source .column-name {
  color: var(--text-secondary);
}

.col-arrow {
  text-align: center;
  color: var(--accent-success);
  font-size: 1rem;
}

.col-target {
  font-family: monospace;
}

.col-target .table-name {
  color: #8b5cf6;
}

.col-target .column-name {
  color: var(--text-secondary);
}

.col-transform {
  font-family: monospace;
  font-size: 0.6875rem;
  color: var(--text-muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* Horizontal Guide Steps */
.guide-steps-horizontal {
  display: flex;
  gap: var(--spacing-md);
  flex-wrap: wrap;
}

.guide-step-h {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  background: var(--bg-primary);
  padding: var(--spacing-sm) var(--spacing-md);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-color);
  flex: 1;
  min-width: 200px;
}

.guide-step-h.highlight {
  border-color: var(--accent-success);
  background: rgba(16, 185, 129, 0.1);
}

.guide-step-h .step-num {
  width: 20px;
  height: 20px;
  font-size: 0.6875rem;
}

.guide-step-h .step-text {
  flex: 1;
  font-size: 0.75rem;
  color: var(--text-secondary);
}

.btn-xs {
  padding: 2px 8px;
  font-size: 0.6875rem;
}

.etl-btn-mini {
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-success));
  color: white;
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 0.625rem;
  font-weight: 500;
}
</style>
