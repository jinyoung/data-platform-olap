<script setup>
import { ref, onMounted, computed, watch, nextTick } from 'vue'
import { useCubeStore } from '../store/cubeStore'
import mermaid from 'mermaid'

const store = useCubeStore()

// API Gateway URL - 모든 마이크로서비스 요청의 단일 진입점
const API_GATEWAY_URL = import.meta.env.VITE_API_GATEWAY_URL ?? 'http://localhost:9000'
const API_BASE_URL = `${API_GATEWAY_URL}/olap/api`

// Initialize Mermaid
onMounted(() => {
  mermaid.initialize({
    startOnLoad: false,
    theme: 'dark',
    themeVariables: {
      primaryColor: '#0d1929',
      primaryTextColor: '#e2e8f0',
      primaryBorderColor: '#00d4ff',
      lineColor: '#00d4ff',
      secondaryColor: '#1a2744',
      tertiaryColor: '#0f1d32'
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
  fetchCatalog()
})

// State
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const catalog = ref({ tables: [], schemas: [], relationships: [] })
const selectedTables = ref([])
const aiSuggestion = ref(null)
const syncResult = ref(null)
const diagramContainer = ref(null)

// Form state
const cubeDescription = ref('')
const dwSchema = ref('dw')
const cubeName = ref('OrderAnalytics')

// Steps state
const currentStep = ref(1)

// Fetch catalog from Neo4j
async function fetchCatalog() {
  loading.value = true
  error.value = null
  
  try {
    const response = await fetch(`${API_BASE_URL}/etl/catalog`)
    if (!response.ok) throw new Error('Failed to fetch catalog')
    catalog.value = await response.json()
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// Get AI suggestion for ETL strategy
async function getSuggestion() {
  if (!cubeDescription.value.trim()) {
    error.value = '큐브 설명을 입력해주세요'
    return
  }
  
  loading.value = true
  error.value = null
  aiSuggestion.value = null
  
  try {
    const response = await fetch(`${API_BASE_URL}/etl/suggest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        cube_description: cubeDescription.value
      })
    })
    if (!response.ok) throw new Error('Failed to get AI suggestion')
    const data = await response.json()
    aiSuggestion.value = data.suggestion
    
    if (aiSuggestion.value && !aiSuggestion.value.error) {
      currentStep.value = 2
      // Auto-select suggested source tables
      selectedTables.value = [
        ...(aiSuggestion.value.fact_sources || []),
        ...Object.values(aiSuggestion.value.dimension_sources || {}).flat()
      ].filter(t => t && !t.startsWith('.'))
      
      // Render diagram after suggestion
      await nextTick()
      await renderMermaidDiagram()
    }
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// Sanitize name for Mermaid (remove special characters)
function sanitizeName(name) {
  if (!name) return 'unknown'
  return name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^_+/, '').replace(/_+$/, '') || 'entity'
}

// Generate Mermaid ER Diagram from AI suggestion
const generateMermaidDiagram = computed(() => {
  if (!aiSuggestion.value) return ''
  
  const suggestion = aiSuggestion.value
  const factTables = suggestion.fact_sources || []
  const dimensions = suggestion.dimension_sources || {}
  
  if (factTables.length === 0) return ''
  
  let diagram = `erDiagram\n`
  
  // Create main fact table
  const factTableName = sanitizeName(`fact_${cubeName.value.toLowerCase().replace(/\s+/g, '_')}`)
  diagram += `    ${factTableName} {\n`
  diagram += `        int id PK\n`
  
  // Add dimension FKs
  Object.keys(dimensions).forEach(dimName => {
    const safeDimName = sanitizeName(dimName)
    const fkCol = sanitizeName(`${safeDimName}_id`)
    diagram += `        int ${fkCol} FK\n`
  })
  
  // Add measures from mappings
  const measures = (suggestion.suggested_mappings || [])
    .filter(m => m.transformation && (m.transformation.includes('SUM') || m.transformation.includes('COUNT') || m.transformation.includes('AVG')))
  
  measures.forEach(m => {
    const colName = sanitizeName(m.target.split('.').pop() || 'measure')
    diagram += `        decimal ${colName}\n`
  })
  
  diagram += `        timestamp created_at\n`
  diagram += `    }\n\n`
  
  // Create dimension tables
  Object.entries(dimensions).forEach(([dimName, sources]) => {
    const safeDimName = sanitizeName(dimName)
    diagram += `    ${safeDimName} {\n`
    diagram += `        int id PK\n`
    
    // Get columns from mappings for this dimension
    const dimMappings = (suggestion.suggested_mappings || [])
      .filter(m => m.target.startsWith(dimName))
    
    dimMappings.forEach(m => {
      const colName = sanitizeName(m.target.split('.').pop() || 'column')
      diagram += `        varchar ${colName}\n`
    })
    
    if (dimMappings.length === 0) {
      diagram += `        varchar name\n`
      diagram += `        varchar description\n`
    }
    
    diagram += `    }\n\n`
  })
  
  // Add relationships
  Object.keys(dimensions).forEach(dimName => {
    const safeDimName = sanitizeName(dimName)
    diagram += `    ${safeDimName} ||--o{ ${factTableName} : has\n`
  })
  
  return diagram
})

// Render Mermaid diagram
async function renderMermaidDiagram() {
  if (!diagramContainer.value || !generateMermaidDiagram.value) return
  
  try {
    const uniqueId = `etl-diagram-${Date.now()}`
    const { svg } = await mermaid.render(uniqueId, generateMermaidDiagram.value)
    diagramContainer.value.innerHTML = svg
  } catch (e) {
    console.error('Mermaid render error:', e)
    diagramContainer.value.innerHTML = `<div class="diagram-error">다이어그램 렌더링 오류</div>`
  }
}

// Watch for diagram changes
watch(generateMermaidDiagram, async () => {
  await nextTick()
  await renderMermaidDiagram()
})

// Create DW schema and tables
async function createDWSchema() {
  loading.value = true
  error.value = null
  
  try {
    const response = await fetch(`${API_BASE_URL}/etl/schema/create?schema_name=${dwSchema.value}`, {
      method: 'POST'
    })
    if (!response.ok) throw new Error('Failed to create schema')
    success.value = `스키마 "${dwSchema.value}" 생성 완료!`
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// Generate XML from AI suggestion and upload cube
async function confirmAndCreateCube() {
  if (!aiSuggestion.value) {
    error.value = 'AI 추천 결과가 없습니다'
    return
  }
  
  loading.value = true
  error.value = null
  
  try {
    // 1. Create DW schema
    await fetch(`${API_BASE_URL}/etl/schema/create?schema_name=${dwSchema.value}`, {
      method: 'POST'
    })
    
    // 2. Generate Mondrian XML from suggestion
    const xml = generateMondrianXML()
    
    // 3. Upload to cube store
    const uploadResult = await store.uploadSchemaText(xml)
    
    // 4. Create ETL config
    const etlConfig = {
      cube_name: cubeName.value,
      fact_table: `fact_${cubeName.value.toLowerCase().replace(/\s+/g, '_')}`,
      dimension_tables: Object.keys(aiSuggestion.value.dimension_sources || {}),
      source_tables: selectedTables.value,
      mappings: (aiSuggestion.value.suggested_mappings || []).map(m => ({
        source_table: m.source.split('.')[0] || '',
        source_column: m.source.split('.').pop() || '',
        target_table: m.target.split('.')[0] || '',
        target_column: m.target.split('.').pop() || '',
        transformation: m.transformation || ''
      })),
      dw_schema: dwSchema.value,
      sync_mode: aiSuggestion.value.sync_strategy || 'full',
      incremental_column: aiSuggestion.value.incremental_column || null
    }
    
    await fetch(`${API_BASE_URL}/etl/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(etlConfig)
    })
    
    currentStep.value = 3
    success.value = `큐브 "${cubeName.value}"가 생성되었습니다! ETL 설정이 완료되었습니다.`
    
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// Generate Mondrian XML from AI suggestion
function generateMondrianXML() {
  const suggestion = aiSuggestion.value
  const factTableName = `${dwSchema.value}.fact_${cubeName.value.toLowerCase().replace(/\s+/g, '_')}`
  
  let xml = `<?xml version="1.0" encoding="UTF-8"?>
<Schema name="${cubeName.value}Schema">
  <Cube name="${cubeName.value}">
    <Table name="${factTableName}"/>
`

  // Add dimensions
  Object.entries(suggestion.dimension_sources || {}).forEach(([dimName, sources]) => {
    const dimTable = `${dwSchema.value}.${dimName}`
    const fk = `${dimName}_id`
    
    xml += `
    <Dimension name="${dimName.replace('dim_', '').replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}" foreignKey="${fk}">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="${dimTable}"/>
        <Level name="Name" column="name"/>
      </Hierarchy>
    </Dimension>
`
  })

  // Add measures
  const measures = (suggestion.suggested_mappings || [])
    .filter(m => m.transformation && (m.transformation.includes('SUM') || m.transformation.includes('COUNT') || m.transformation.includes('AVG')))
  
  measures.forEach(m => {
    const colName = m.target.split('.').pop() || 'measure'
    const aggregator = m.transformation.includes('SUM') ? 'sum' : 
                       m.transformation.includes('COUNT') ? 'count' : 
                       m.transformation.includes('AVG') ? 'avg' : 'sum'
    const measureName = colName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
    
    xml += `
    <Measure name="${measureName}" column="${colName}" aggregator="${aggregator}" formatString="#,###"/>
`
  })
  
  // Add default measure if none
  if (measures.length === 0) {
    xml += `
    <Measure name="Count" column="id" aggregator="count" formatString="#,###"/>
`
  }

  xml += `  </Cube>
</Schema>`

  return xml
}

// Execute ETL sync
async function executeSync() {
  loading.value = true
  error.value = null
  syncResult.value = null
  
  try {
    const response = await fetch(`${API_BASE_URL}/etl/sync/${cubeName.value}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force_full: true })
    })
    if (!response.ok) throw new Error('Failed to execute sync')
    syncResult.value = await response.json()
    
    if (syncResult.value.status === 'completed') {
      success.value = `ETL 동기화 완료! ${syncResult.value.rows_inserted}행 삽입됨`
    }
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// Group tables by schema
const tablesBySchema = computed(() => {
  const grouped = {}
  for (const table of catalog.value.tables || []) {
    const schema = table.schema || 'public'
    if (!grouped[schema]) grouped[schema] = []
    grouped[schema].push(table)
  }
  return grouped
})

// Toggle table selection
function toggleTableSelection(tableName) {
  const idx = selectedTables.value.indexOf(tableName)
  if (idx >= 0) {
    selectedTables.value.splice(idx, 1)
  } else {
    selectedTables.value.push(tableName)
  }
}
</script>

<template>
  <div class="etl-manager">
    <!-- Header -->
    <div class="etl-header">
      <h2>🔄 ETL 데이터 파이프라인</h2>
      <p class="subtitle">OLTP 원본 테이블에서 OLAP 스타 스키마로 데이터를 동기화합니다</p>
    </div>
    
    <!-- Progress Steps -->
    <div class="progress-steps">
      <div :class="['step', { active: currentStep >= 1, current: currentStep === 1 }]">
        <span class="step-number">1</span>
        <span class="step-label">큐브 설계</span>
      </div>
      <div class="step-line" :class="{ active: currentStep >= 2 }"></div>
      <div :class="['step', { active: currentStep >= 2, current: currentStep === 2 }]">
        <span class="step-number">2</span>
        <span class="step-label">스키마 확인</span>
      </div>
      <div class="step-line" :class="{ active: currentStep >= 3 }"></div>
      <div :class="['step', { active: currentStep >= 3, current: currentStep === 3 }]">
        <span class="step-number">3</span>
        <span class="step-label">ETL 실행</span>
      </div>
    </div>
    
    <!-- Error/Success Alerts -->
    <div v-if="error" class="alert error">
      <span class="icon">⚠️</span>
      {{ error }}
      <button @click="error = null">×</button>
    </div>
    <div v-if="success" class="alert success">
      <span class="icon">✅</span>
      {{ success }}
      <button @click="success = null">×</button>
    </div>
    
    <!-- Loading -->
    <div v-if="loading" class="loading-overlay">
      <div class="spinner"></div>
      <span>처리 중...</span>
    </div>
    
    <!-- Step 1: Cube Description -->
    <div v-if="currentStep === 1" class="step-content">
      <div class="content-grid">
        <!-- Left: Catalog -->
        <div class="panel catalog-panel">
          <div class="panel-header">
            <h3>📚 원본 테이블 카탈로그</h3>
            <button class="btn-icon" @click="fetchCatalog" title="새로고침">🔄</button>
          </div>
          
          <div class="catalog-stats">
            <div class="stat">
              <span class="stat-value">{{ catalog.tables?.length || 0 }}</span>
              <span class="stat-label">테이블</span>
            </div>
            <div class="stat">
              <span class="stat-value">{{ catalog.schemas?.length || 0 }}</span>
              <span class="stat-label">스키마</span>
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
                  :class="{ selected: selectedTables.includes(table.name) }"
                  :title="table.description || '설명 없음'"
                  @click="toggleTableSelection(table.name)"
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
        
        <!-- Right: Cube Description -->
        <div class="panel design-panel">
          <div class="panel-header">
            <h3>🤖 AI 큐브 설계</h3>
          </div>
          
          <div class="design-form">
            <div class="form-group">
              <label>큐브 이름</label>
              <input v-model="cubeName" placeholder="예: OrderAnalytics" />
            </div>
            
            <div class="form-group">
              <label>DW 스키마</label>
              <input v-model="dwSchema" placeholder="dw" />
            </div>
            
            <div class="form-group">
              <label>큐브 설명</label>
              <textarea 
                v-model="cubeDescription" 
                placeholder="예: 총 주문에 대한 것들을 어떤 제품이 많은 주문이 이루어졌는지를 볼 수 있는 스키마를 살펴보자."
                rows="5"
              ></textarea>
            </div>
            
            <button class="btn primary large" @click="getSuggestion" :disabled="loading || !cubeDescription.trim()">
              <span>⚡</span> AI 전략 추천받기
            </button>
          </div>
        </div>
      </div>
    </div>
    
    <!-- Step 2: Review Suggestion & Star Schema -->
    <div v-if="currentStep === 2 && aiSuggestion" class="step-content">
      <div class="suggestion-grid">
        <!-- Left: Formatted Suggestion -->
        <div class="panel suggestion-panel">
          <div class="panel-header">
            <h3>📋 AI 추천 ETL 전략</h3>
            <button class="btn secondary small" @click="currentStep = 1">← 수정하기</button>
          </div>
          
          <div class="suggestion-content">
            <!-- Strategy Overview -->
            <div class="suggestion-section">
              <h4>📌 동기화 전략</h4>
              <div class="strategy-badge" :class="aiSuggestion.sync_strategy">
                {{ aiSuggestion.sync_strategy === 'incremental' ? '🔄 증분 동기화' : '🔃 전체 새로고침' }}
              </div>
              <p v-if="aiSuggestion.incremental_column" class="strategy-detail">
                기준 컬럼: <code>{{ aiSuggestion.incremental_column }}</code>
              </p>
            </div>
            
            <!-- Fact Sources -->
            <div class="suggestion-section">
              <h4>📊 팩트 테이블 원본</h4>
              <ul class="source-list">
                <li v-for="source in aiSuggestion.fact_sources" :key="source">
                  <span class="source-icon">📈</span>
                  {{ source }}
                </li>
              </ul>
            </div>
            
            <!-- Dimension Sources -->
            <div class="suggestion-section">
              <h4>📁 차원 테이블 매핑</h4>
              <div class="dimension-list">
                <div v-for="(sources, dimName) in aiSuggestion.dimension_sources" :key="dimName" class="dimension-item">
                  <div class="dim-header">
                    <span class="dim-icon">🏷️</span>
                    <strong>{{ dimName }}</strong>
                  </div>
                  <div class="dim-sources">
                    ← <span v-for="(s, i) in sources" :key="s">{{ s }}<span v-if="i < sources.length - 1">, </span></span>
                  </div>
                </div>
              </div>
            </div>
            
            <!-- Mappings Summary -->
            <div class="suggestion-section">
              <h4>🔗 컬럼 매핑 ({{ aiSuggestion.suggested_mappings?.length || 0 }}개)</h4>
              <div class="mappings-table">
                <div class="mapping-row header">
                  <span>원본</span>
                  <span>→</span>
                  <span>대상</span>
                  <span>변환</span>
                </div>
                <div 
                  v-for="(m, i) in aiSuggestion.suggested_mappings?.slice(0, 8)" 
                  :key="i" 
                  class="mapping-row"
                >
                  <span class="mapping-source">{{ m.source }}</span>
                  <span class="mapping-arrow">→</span>
                  <span class="mapping-target">{{ m.target }}</span>
                  <span class="mapping-transform">{{ m.transformation || '-' }}</span>
                </div>
                <div v-if="aiSuggestion.suggested_mappings?.length > 8" class="mapping-more">
                  ... 외 {{ aiSuggestion.suggested_mappings.length - 8 }}개 매핑
                </div>
              </div>
            </div>
            
            <!-- Reasoning -->
            <div v-if="aiSuggestion.reasoning" class="suggestion-section reasoning">
              <h4>💡 AI 분석</h4>
              <p>{{ aiSuggestion.reasoning }}</p>
            </div>
          </div>
        </div>
        
        <!-- Right: Star Schema Diagram -->
        <div class="panel schema-panel">
          <div class="panel-header">
            <h3>⭐ 스타 스키마 설계</h3>
          </div>
          
          <div class="diagram-container">
            <div ref="diagramContainer" class="mermaid-container">
              <div v-if="!generateMermaidDiagram" class="diagram-placeholder">
                <p>스키마를 생성 중...</p>
              </div>
            </div>
            
            <div class="diagram-legend">
              <div class="legend-item">
                <span class="legend-icon fact">◼</span>
                <span>팩트 테이블</span>
              </div>
              <div class="legend-item">
                <span class="legend-icon dim">◼</span>
                <span>차원 테이블</span>
              </div>
            </div>
          </div>
          
          <!-- Action Buttons -->
          <div class="schema-actions">
            <button class="btn primary large" @click="confirmAndCreateCube" :disabled="loading">
              <span>✅</span> 확인 및 스키마 생성
            </button>
          </div>
        </div>
      </div>
    </div>
    
    <!-- Step 3: ETL Execution -->
    <div v-if="currentStep === 3" class="step-content">
      <div class="execution-panel">
        <div class="panel">
          <div class="panel-header">
            <h3>🚀 ETL 실행</h3>
          </div>
          
          <div class="execution-content">
            <div class="success-banner">
              <span class="success-icon">✅</span>
              <div>
                <strong>큐브 "{{ cubeName }}" 설정 완료!</strong>
                <p>ETL 파이프라인이 준비되었습니다. 동기화를 실행하여 OLAP 데이터를 생성하세요.</p>
              </div>
            </div>
            
            <div class="execution-actions">
              <button class="btn primary large" @click="executeSync" :disabled="loading">
                <span>▶️</span> ETL 동기화 실행
              </button>
              
              <button class="btn secondary" @click="currentStep = 2">
                ← 스키마 검토
              </button>
            </div>
            
            <!-- Sync Result -->
            <div v-if="syncResult" class="sync-result" :class="syncResult.status">
              <div class="result-header">
                <span class="result-icon">{{ syncResult.status === 'completed' ? '✅' : '❌' }}</span>
                <strong>{{ syncResult.status === 'completed' ? '동기화 완료' : '동기화 실패' }}</strong>
              </div>
              <div class="result-stats">
                <div class="result-stat">
                  <span class="label">삽입된 행:</span>
                  <span class="value">{{ syncResult.rows_inserted }}</span>
                </div>
                <div class="result-stat">
                  <span class="label">업데이트된 행:</span>
                  <span class="value">{{ syncResult.rows_updated }}</span>
                </div>
                <div class="result-stat">
                  <span class="label">소요 시간:</span>
                  <span class="value">{{ syncResult.duration_ms }}ms</span>
                </div>
              </div>
              <div v-if="syncResult.error" class="result-error">
                {{ syncResult.error }}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.etl-manager {
  padding: var(--spacing-lg);
  min-height: 100%;
}

.etl-header {
  margin-bottom: var(--spacing-lg);
}

.etl-header h2 {
  font-size: 1.75rem;
  margin-bottom: var(--spacing-xs);
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.subtitle {
  color: var(--text-muted);
}

/* Progress Steps */
.progress-steps {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-xs);
  margin-bottom: var(--spacing-xl);
  padding: var(--spacing-lg);
  background: var(--bg-secondary);
  border-radius: var(--radius-lg);
}

.step {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-md);
  border-radius: var(--radius-md);
  opacity: 0.5;
  transition: all var(--transition-fast);
}

.step.active {
  opacity: 1;
}

.step.current {
  background: rgba(0, 212, 255, 0.1);
  border: 1px solid var(--accent-primary);
}

.step-number {
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-tertiary);
  border-radius: 50%;
  font-weight: 600;
  font-size: 0.875rem;
}

.step.current .step-number {
  background: var(--accent-primary);
  color: white;
}

.step-label {
  font-size: 0.875rem;
  font-weight: 500;
}

.step-line {
  width: 60px;
  height: 2px;
  background: var(--border-color);
  transition: background var(--transition-fast);
}

.step-line.active {
  background: var(--accent-primary);
}

/* Alerts */
.alert {
  display: flex;
  align-items: center;
  gap: var(--spacing-md);
  padding: var(--spacing-md);
  border-radius: var(--radius-md);
  margin-bottom: var(--spacing-lg);
}

.alert.error {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid var(--accent-error);
  color: var(--accent-error);
}

.alert.success {
  background: rgba(16, 185, 129, 0.1);
  border: 1px solid var(--accent-success);
  color: var(--accent-success);
}

.alert button {
  margin-left: auto;
  background: none;
  border: none;
  color: inherit;
  font-size: 1.25rem;
  cursor: pointer;
}

/* Loading */
.loading-overlay {
  position: fixed;
  inset: 0;
  background: rgba(10, 14, 23, 0.8);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-md);
  z-index: 1000;
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

/* Content Grid */
.step-content {
  animation: fadeIn 0.3s ease;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

.content-grid {
  display: grid;
  grid-template-columns: 1fr 1.5fr;
  gap: var(--spacing-lg);
}

.suggestion-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-lg);
}

/* Panels */
.panel {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
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

/* Catalog Panel */
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
  max-height: 400px;
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
  border: 1px solid transparent;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.table-item:hover {
  background: var(--bg-elevated);
  border-color: var(--border-color);
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

.table-item.selected {
  background: rgba(0, 212, 255, 0.1);
  border: 1px solid var(--accent-primary);
}

.table-item.selected .table-name {
  color: var(--accent-primary);
}

/* Design Panel */
.design-form {
  padding: var(--spacing-lg);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-xs);
}

.form-group label {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--text-secondary);
}

.form-group input,
.form-group textarea {
  padding: var(--spacing-md);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 0.9375rem;
}

.form-group input:focus,
.form-group textarea:focus {
  outline: none;
  border-color: var(--accent-primary);
}

/* Buttons */
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
  transition: all var(--transition-fast);
}

.btn.primary {
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  color: white;
}

.btn.primary:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(0, 212, 255, 0.3);
}

.btn.secondary {
  background: var(--bg-tertiary);
  color: var(--text-primary);
  border: 1px solid var(--border-color);
}

.btn.large {
  padding: var(--spacing-md) var(--spacing-xl);
  font-size: 1rem;
}

.btn.small {
  padding: var(--spacing-xs) var(--spacing-md);
  font-size: 0.8125rem;
}

.btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-icon {
  background: none;
  border: none;
  font-size: 1.25rem;
  cursor: pointer;
  padding: var(--spacing-xs);
}

/* Suggestion Panel */
.suggestion-content {
  padding: var(--spacing-lg);
  max-height: 600px;
  overflow-y: auto;
}

.suggestion-section {
  margin-bottom: var(--spacing-lg);
  padding-bottom: var(--spacing-lg);
  border-bottom: 1px solid var(--border-color);
}

.suggestion-section:last-child {
  border-bottom: none;
  margin-bottom: 0;
  padding-bottom: 0;
}

.suggestion-section h4 {
  font-size: 0.875rem;
  color: var(--accent-primary);
  margin-bottom: var(--spacing-md);
}

.strategy-badge {
  display: inline-block;
  padding: var(--spacing-sm) var(--spacing-md);
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
  font-weight: 500;
}

.strategy-badge.incremental {
  background: rgba(16, 185, 129, 0.1);
  color: var(--accent-success);
  border: 1px solid var(--accent-success);
}

.strategy-detail {
  margin-top: var(--spacing-sm);
  font-size: 0.875rem;
  color: var(--text-muted);
}

.strategy-detail code {
  background: var(--bg-tertiary);
  padding: 2px 6px;
  border-radius: 4px;
  font-family: monospace;
  color: var(--accent-primary);
}

.source-list {
  list-style: none;
  padding: 0;
  margin: 0;
}

.source-list li {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-xs) 0;
}

.dimension-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.dimension-item {
  background: var(--bg-tertiary);
  padding: var(--spacing-sm) var(--spacing-md);
  border-radius: var(--radius-md);
}

.dim-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  margin-bottom: var(--spacing-xs);
}

.dim-sources {
  font-size: 0.8125rem;
  color: var(--text-muted);
  padding-left: 1.5rem;
}

/* Mappings Table */
.mappings-table {
  font-size: 0.8125rem;
}

.mapping-row {
  display: grid;
  grid-template-columns: 2fr 0.5fr 2fr 1.5fr;
  gap: var(--spacing-sm);
  padding: var(--spacing-xs) var(--spacing-sm);
  border-radius: var(--radius-sm);
}

.mapping-row.header {
  font-weight: 600;
  color: var(--text-muted);
  text-transform: uppercase;
  font-size: 0.7rem;
  letter-spacing: 0.05em;
}

.mapping-row:not(.header):hover {
  background: var(--bg-tertiary);
}

.mapping-source {
  color: var(--text-secondary);
}

.mapping-arrow {
  color: var(--accent-primary);
  text-align: center;
}

.mapping-target {
  color: var(--accent-success);
}

.mapping-transform {
  color: var(--text-muted);
  font-family: monospace;
  font-size: 0.75rem;
}

.mapping-more {
  text-align: center;
  padding: var(--spacing-sm);
  color: var(--text-muted);
  font-style: italic;
}

.reasoning {
  background: var(--bg-tertiary);
  padding: var(--spacing-md);
  border-radius: var(--radius-md);
  margin-top: var(--spacing-md);
}

.reasoning p {
  font-size: 0.875rem;
  line-height: 1.6;
  color: var(--text-secondary);
}

/* Schema Panel */
.diagram-container {
  padding: var(--spacing-lg);
  min-height: 300px;
}

.mermaid-container {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 250px;
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  padding: var(--spacing-lg);
}

.mermaid-container svg {
  max-width: 100%;
  height: auto;
}

.diagram-placeholder {
  color: var(--text-muted);
}

.diagram-legend {
  display: flex;
  gap: var(--spacing-lg);
  justify-content: center;
  padding: var(--spacing-md);
  border-top: 1px solid var(--border-color);
}

.legend-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  font-size: 0.75rem;
  color: var(--text-muted);
}

.legend-icon.fact {
  color: var(--accent-primary);
}

.legend-icon.dim {
  color: var(--accent-secondary);
}

.schema-actions {
  padding: var(--spacing-lg);
  border-top: 1px solid var(--border-color);
  text-align: center;
}

/* Execution Panel */
.execution-panel {
  max-width: 800px;
  margin: 0 auto;
}

.execution-content {
  padding: var(--spacing-xl);
}

.success-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-md);
  padding: var(--spacing-lg);
  background: rgba(16, 185, 129, 0.1);
  border: 1px solid var(--accent-success);
  border-radius: var(--radius-lg);
  margin-bottom: var(--spacing-xl);
}

.success-icon {
  font-size: 2rem;
}

.success-banner strong {
  color: var(--accent-success);
  font-size: 1.125rem;
}

.success-banner p {
  color: var(--text-secondary);
  margin-top: var(--spacing-xs);
}

.execution-actions {
  display: flex;
  gap: var(--spacing-md);
  margin-bottom: var(--spacing-xl);
}

/* Sync Result */
.sync-result {
  padding: var(--spacing-lg);
  border-radius: var(--radius-lg);
  margin-top: var(--spacing-lg);
}

.sync-result.completed {
  background: rgba(16, 185, 129, 0.1);
  border: 1px solid var(--accent-success);
}

.sync-result.failed {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid var(--accent-error);
}

.result-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  margin-bottom: var(--spacing-md);
}

.result-icon {
  font-size: 1.5rem;
}

.result-stats {
  display: flex;
  gap: var(--spacing-xl);
}

.result-stat {
  display: flex;
  flex-direction: column;
}

.result-stat .label {
  font-size: 0.75rem;
  color: var(--text-muted);
}

.result-stat .value {
  font-size: 1.5rem;
  font-weight: 600;
  color: var(--accent-primary);
}

.result-error {
  margin-top: var(--spacing-md);
  padding: var(--spacing-md);
  background: rgba(239, 68, 68, 0.1);
  border-radius: var(--radius-sm);
  color: var(--accent-error);
}

@media (max-width: 1200px) {
  .content-grid,
  .suggestion-grid {
    grid-template-columns: 1fr;
  }
}
</style>
