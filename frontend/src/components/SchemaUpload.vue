<script setup>
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useCubeStore } from '../store/cubeStore'

const { t } = useI18n()
const store = useCubeStore()
const isDragging = ref(false)
const showTextInput = ref(false)
const xmlText = ref('')

const handleDrop = async (e) => {
  isDragging.value = false
  const file = e.dataTransfer?.files[0]
  if (file && file.name.endsWith('.xml')) {
    await store.uploadSchema(file)
  }
}

const handleFileSelect = async (e) => {
  const file = e.target.files[0]
  if (file) {
    await store.uploadSchema(file)
  }
}

const handleTextUpload = async () => {
  if (xmlText.value.trim()) {
    await store.uploadSchemaText(xmlText.value)
    xmlText.value = ''
    showTextInput.value = false
  }
}

// Sample schema for demo
const loadSampleSchema = async () => {
  const sampleXML = `<?xml version="1.0" encoding="UTF-8"?>
<Schema name="SalesAnalysis">
  <Cube name="Sales">
    <Table name="fact_sales"/>
    
    <Dimension name="Date" foreignKey="date_id">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="dim_date"/>
        <Level name="Year" column="year" type="Numeric"/>
        <Level name="Quarter" column="quarter"/>
        <Level name="Month" column="month"/>
        <Level name="Day" column="day"/>
      </Hierarchy>
    </Dimension>
    
    <Dimension name="Product" foreignKey="product_id">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="dim_product"/>
        <Level name="Category" column="category"/>
        <Level name="SubCategory" column="subcategory"/>
        <Level name="ProductName" column="product_name"/>
      </Hierarchy>
    </Dimension>
    
    <Dimension name="Region" foreignKey="region_id">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="dim_region"/>
        <Level name="Country" column="country"/>
        <Level name="State" column="state"/>
        <Level name="City" column="city"/>
      </Hierarchy>
    </Dimension>
    
    <Dimension name="Customer" foreignKey="customer_id">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="dim_customer"/>
        <Level name="Segment" column="segment"/>
        <Level name="CustomerName" column="customer_name"/>
      </Hierarchy>
    </Dimension>
    
    <Measure name="SalesAmount" column="sales_amount" aggregator="sum" formatString="#,###"/>
    <Measure name="Quantity" column="quantity" aggregator="sum" formatString="#,###"/>
    <Measure name="Profit" column="profit" aggregator="sum" formatString="#,###.00"/>
    <Measure name="Discount" column="discount" aggregator="avg" formatString="0.00%"/>
    <Measure name="OrderCount" column="order_id" aggregator="distinct-count" formatString="#,###"/>
  </Cube>
</Schema>`
  
  await store.uploadSchemaText(sampleXML)
}
</script>

<template>
  <div class="schema-upload">
    <div 
      :class="['upload-zone', { 'drag-over': isDragging }]"
      @dragover.prevent="isDragging = true"
      @dragleave="isDragging = false"
      @drop.prevent="handleDrop"
    >
      <div class="upload-icon">
        <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
          <polyline points="17 8 12 3 7 8"/>
          <line x1="12" y1="3" x2="12" y2="15"/>
        </svg>
      </div>
      <p class="upload-text">{{ t('schema.dropZone') }}</p>
      <p class="upload-hint">{{ t('schema.or') }}</p>
      
      <div class="upload-actions">
        <label class="btn btn-primary">
          <input type="file" accept=".xml" @change="handleFileSelect" hidden />
          {{ t('schema.browseFiles') }}
        </label>
        <button class="btn btn-secondary" @click="showTextInput = !showTextInput">
          {{ t('schema.pasteXml') }}
        </button>
      </div>
    </div>
    
    <!-- Text Input Modal -->
    <div v-if="showTextInput" class="text-input-panel fade-in">
      <div class="panel-header">
        <h4>{{ t('schema.pasteXml') }}</h4>
        <button class="btn btn-ghost" @click="showTextInput = false">×</button>
      </div>
      <textarea 
        v-model="xmlText"
        class="textarea xml-input"
        :placeholder="t('schema.pasteXml') + '...'"
        rows="10"
      ></textarea>
      <div class="panel-actions">
        <button class="btn btn-secondary" @click="showTextInput = false">{{ t('common.cancel') }}</button>
        <button class="btn btn-primary" @click="handleTextUpload" :disabled="!xmlText.trim()">
          {{ t('common.upload') }}
        </button>
      </div>
    </div>
    
    <!-- Sample Data Button -->
    <button class="btn btn-ghost sample-btn" @click="loadSampleSchema">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
        <polyline points="14 2 14 8 20 8"/>
        <line x1="16" y1="13" x2="8" y2="13"/>
        <line x1="16" y1="17" x2="8" y2="17"/>
        <polyline points="10 9 9 9 8 9"/>
      </svg>
      {{ t('schema.loadSample') }}
    </button>
  </div>
</template>

<style scoped>
.schema-upload {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-md);
}

.upload-zone {
  background: var(--bg-secondary);
  border: 2px dashed var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-xl);
  text-align: center;
  transition: all var(--transition-fast);
}

.upload-zone.drag-over {
  border-color: var(--accent-primary);
  background: rgba(0, 212, 255, 0.05);
  box-shadow: inset 0 0 30px rgba(0, 212, 255, 0.1);
}

.upload-icon {
  color: var(--text-muted);
  margin-bottom: var(--spacing-md);
}

.upload-text {
  color: var(--text-secondary);
  font-weight: 500;
  margin-bottom: var(--spacing-xs);
}

.upload-hint {
  color: var(--text-muted);
  font-size: 0.75rem;
  margin-bottom: var(--spacing-md);
}

.upload-actions {
  display: flex;
  gap: var(--spacing-sm);
  justify-content: center;
}

.text-input-panel {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: var(--spacing-md);
}

.panel-header h4 {
  font-size: 0.875rem;
  color: var(--text-secondary);
}

.xml-input {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  resize: vertical;
  min-height: 200px;
}

.panel-actions {
  display: flex;
  gap: var(--spacing-sm);
  justify-content: flex-end;
  margin-top: var(--spacing-md);
}

.sample-btn {
  width: 100%;
  justify-content: center;
}
</style>

