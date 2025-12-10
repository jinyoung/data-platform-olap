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
    await store.uploadSchemaText(xml)
    success.value = 'Cube uploaded successfully!'
    
    // Reset form on success
    if (activeTab.value === 'visual') {
      cubeName.value = ''
      factTable.value = ''
      dimensions.value = []
      measures.value = []
    } else {
      promptText.value = ''
      generatedXML.value = ''
    }
  } catch (e) {
    error.value = e.response?.data?.detail || e.message || 'Failed to upload cube'
  } finally {
    loading.value = false
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
    <div v-if="success" class="message success">
      <span>✅ {{ success }}</span>
      <button @click="success = null">×</button>
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

.xml-preview {
  flex: 1;
  padding: var(--spacing-md);
  margin: 0;
  overflow: auto;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  line-height: 1.6;
  color: var(--text-secondary);
  background: var(--bg-primary);
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

