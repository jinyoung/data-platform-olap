<script setup>
import { ref, computed } from 'vue'
import { useCubeStore } from '../store/cubeStore'
import * as api from '../services/api'

const store = useCubeStore()
const activeTab = ref('visual') // 'visual' or 'prompt'
const loading = ref(false)
const error = ref(null)
const success = ref(null)

// Visual Modeler State
const cubeName = ref('')
const factTable = ref('')
const dimensions = ref([])
const measures = ref([])

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

// Generate table DDL and sample data
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
    const response = await fetch(`http://localhost:8000/api/cube/${uploadedCubeName.value}/generate-tables?sample_rows=${sampleRowCount.value}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    })
    
    const data = await response.json()
    
    if (data.error) {
      error.value = data.error
    } else {
      generatedSQL.value = data.sql
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
    const response = await fetch('http://localhost:8000/api/cube/execute-sql', {
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
    const response = await fetch('http://localhost:8000/api/cube/generate', {
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
            <div v-if="generatedSQL" class="sql-preview-container">
              <div class="preview-label">Generated SQL:</div>
              <pre class="sql-preview">{{ generatedSQL }}</pre>
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
      <!-- Visual Modeler Tab -->
      <div v-if="activeTab === 'visual'" class="visual-modeler">
        <div class="modeler-form">
          <!-- Cube Basic Info -->
          <div class="form-section">
            <div class="section-header">
              <h3>
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                  <polyline points="3.27 6.96 12 12.01 20.73 6.96"/>
                  <line x1="12" y1="22.08" x2="12" y2="12"/>
                </svg>
                Cube Definition
              </h3>
              <button class="btn btn-ghost" @click="loadSampleTemplate">
                Load Sample
              </button>
            </div>
            <div class="form-grid">
              <div class="form-group">
                <label>Cube Name</label>
                <input v-model="cubeName" type="text" class="input" placeholder="e.g., SalesCube" />
              </div>
              <div class="form-group">
                <label>Fact Table</label>
                <input v-model="factTable" type="text" class="input" placeholder="e.g., fact_sales" />
              </div>
            </div>
          </div>
          
          <!-- Dimensions -->
          <div class="form-section">
            <div class="section-header">
              <h3>
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <rect x="3" y="3" width="18" height="18" rx="2"/>
                  <path d="M3 9h18"/>
                  <path d="M9 21V9"/>
                </svg>
                Dimensions
              </h3>
              <button class="btn btn-secondary" @click="addDimension">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="12" y1="5" x2="12" y2="19"/>
                  <line x1="5" y1="12" x2="19" y2="12"/>
                </svg>
                Add Dimension
              </button>
            </div>
            
            <div v-if="dimensions.length === 0" class="empty-placeholder">
              No dimensions defined. Click "Add Dimension" to start.
            </div>
            
            <div v-for="(dim, dimIndex) in dimensions" :key="dim.id" class="dimension-card">
              <div class="card-header">
                <span class="dim-badge">Dim {{ dimIndex + 1 }}</span>
                <button class="remove-btn" @click="removeDimension(dimIndex)" title="Remove Dimension">×</button>
              </div>
              <div class="form-grid">
                <div class="form-group">
                  <label>Name</label>
                  <input v-model="dim.name" type="text" class="input" placeholder="e.g., Product" />
                </div>
                <div class="form-group">
                  <label>Table</label>
                  <input v-model="dim.table" type="text" class="input" placeholder="e.g., dim_product" />
                </div>
                <div class="form-group">
                  <label>Foreign Key</label>
                  <input v-model="dim.foreignKey" type="text" class="input" placeholder="e.g., product_id" />
                </div>
              </div>
              
              <!-- Levels -->
              <div class="levels-section">
                <div class="levels-header">
                  <span class="levels-label">Hierarchy Levels</span>
                  <button class="btn btn-ghost btn-sm" @click="addLevel(dimIndex)">+ Add Level</button>
                </div>
                <div class="levels-list">
                  <div v-for="(level, levelIndex) in dim.levels" :key="level.id" class="level-row">
                    <span class="level-number">L{{ levelIndex + 1 }}</span>
                    <input v-model="level.name" type="text" class="input" placeholder="Level Name" />
                    <input v-model="level.column" type="text" class="input" placeholder="Column" />
                    <button 
                      class="remove-btn-sm" 
                      @click="removeLevel(dimIndex, levelIndex)"
                      :disabled="dim.levels.length <= 1"
                    >×</button>
                  </div>
                </div>
              </div>
            </div>
          </div>
          
          <!-- Measures -->
          <div class="form-section">
            <div class="section-header">
              <h3>
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="12" y1="20" x2="12" y2="10"/>
                  <line x1="18" y1="20" x2="18" y2="4"/>
                  <line x1="6" y1="20" x2="6" y2="16"/>
                </svg>
                Measures
              </h3>
              <button class="btn btn-secondary" @click="addMeasure">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="12" y1="5" x2="12" y2="19"/>
                  <line x1="5" y1="12" x2="19" y2="12"/>
                </svg>
                Add Measure
              </button>
            </div>
            
            <div v-if="measures.length === 0" class="empty-placeholder">
              No measures defined. Click "Add Measure" to start.
            </div>
            
            <div class="measures-list">
              <div v-for="(measure, index) in measures" :key="measure.id" class="measure-row">
                <span class="measure-badge">Σ</span>
                <input v-model="measure.name" type="text" class="input" placeholder="Measure Name" />
                <input v-model="measure.column" type="text" class="input" placeholder="Column" />
                <select v-model="measure.aggregator" class="select">
                  <option value="sum">SUM</option>
                  <option value="count">COUNT</option>
                  <option value="avg">AVG</option>
                  <option value="min">MIN</option>
                  <option value="max">MAX</option>
                  <option value="distinct-count">DISTINCT COUNT</option>
                </select>
                <button class="remove-btn-sm" @click="removeMeasure(index)">×</button>
              </div>
            </div>
          </div>
        </div>
        
        <!-- XML Preview -->
        <div class="xml-preview-panel">
          <div class="preview-header">
            <h4>Generated XML Preview</h4>
            <button class="btn btn-primary" @click="uploadCube" :disabled="loading || !cubeName || !factTable">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="17 8 12 3 7 8"/>
                <line x1="12" y1="3" x2="12" y2="15"/>
              </svg>
              {{ loading ? 'Uploading...' : 'Upload Cube' }}
            </button>
          </div>
          <pre class="xml-preview">{{ previewXML || '<!-- Define cube to see preview -->' }}</pre>
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
            <h4>Generated Cube XML</h4>
            <button class="btn btn-primary" @click="uploadCube" :disabled="loading">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="17 8 12 3 7 8"/>
                <line x1="12" y1="3" x2="12" y2="15"/>
              </svg>
              {{ loading ? 'Uploading...' : 'Upload Cube' }}
            </button>
          </div>
          <pre class="xml-preview">{{ generatedXML }}</pre>
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
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: var(--spacing-xs);
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

/* Visual Modeler */
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
</style>
