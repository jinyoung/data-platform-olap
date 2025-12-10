<script setup>
import { ref } from 'vue'
import { useCubeStore } from '../store/cubeStore'

const store = useCubeStore()
const question = ref('')
const queryHistory = ref([])

const submitQuestion = async () => {
  if (!question.value.trim()) return
  
  const q = question.value.trim()
  
  try {
    const result = await store.executeNaturalQuery(q)
    
    // Add to history
    queryHistory.value.unshift({
      question: q,
      sql: result.sql,
      success: !result.error,
      timestamp: new Date()
    })
    
    // Keep only last 10 queries
    if (queryHistory.value.length > 10) {
      queryHistory.value.pop()
    }
    
    question.value = ''
  } catch (e) {
    console.error(e)
  }
}

const loadFromHistory = (item) => {
  question.value = item.question
}

// Example questions
const examples = [
  "Show me total sales by year",
  "What are the top 10 products by revenue?",
  "Monthly sales trend for 2024",
  "Sales by region and product category",
  "Average profit margin by customer segment"
]

const setExample = (ex) => {
  question.value = ex
}
</script>

<template>
  <div class="natural-query">
    <div class="query-panel">
      <div class="query-header">
        <h3>Ask a Question</h3>
        <p class="hint">Use natural language to query your data. The AI will convert it to SQL.</p>
      </div>
      
      <!-- Query Input -->
      <div class="query-input-container">
        <div class="input-wrapper">
          <svg class="input-icon" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="11" cy="11" r="8"/>
            <path d="m21 21-4.3-4.3"/>
          </svg>
          <input
            v-model="question"
            type="text"
            class="query-input"
            placeholder="e.g., Show me total sales by month for 2024..."
            @keyup.enter="submitQuestion"
          />
          <button 
            class="submit-btn"
            @click="submitQuestion"
            :disabled="!question.trim() || store.loading"
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="22" y1="2" x2="11" y2="13"/>
              <polygon points="22 2 15 22 11 13 2 9 22 2"/>
            </svg>
          </button>
        </div>
      </div>
      
      <!-- Example Questions -->
      <div class="examples">
        <span class="examples-label">Try:</span>
        <button 
          v-for="ex in examples" 
          :key="ex"
          class="example-btn"
          @click="setExample(ex)"
        >
          {{ ex }}
        </button>
      </div>
    </div>
    
    <!-- Query History -->
    <div v-if="queryHistory.length" class="history-panel">
      <h4>Recent Queries</h4>
      <div class="history-list">
        <div 
          v-for="(item, index) in queryHistory" 
          :key="index"
          :class="['history-item', { error: !item.success }]"
          @click="loadFromHistory(item)"
        >
          <div class="history-question">{{ item.question }}</div>
          <div class="history-meta">
            <span :class="['status', item.success ? 'success' : 'error']">
              {{ item.success ? '✓' : '✗' }}
            </span>
            <span class="time">{{ item.timestamp.toLocaleTimeString() }}</span>
          </div>
        </div>
      </div>
    </div>
    
    <!-- Tips -->
    <div class="tips-panel">
      <h4>💡 Tips for Better Results</h4>
      <ul>
        <li>Be specific about <strong>time periods</strong> (e.g., "in 2024", "last quarter")</li>
        <li>Mention the <strong>dimensions</strong> you want to group by</li>
        <li>Specify <strong>measures</strong> like "total sales", "average profit"</li>
        <li>Use <strong>filters</strong> like "where region is North America"</li>
        <li>Ask for <strong>rankings</strong> with "top 10" or "bottom 5"</li>
      </ul>
    </div>
  </div>
</template>

<style scoped>
.natural-query {
  display: grid;
  grid-template-columns: 1fr 300px;
  grid-template-rows: auto auto;
  gap: var(--spacing-lg);
}

.query-panel {
  grid-column: 1;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-xl);
}

.query-header {
  margin-bottom: var(--spacing-lg);
}

.query-header h3 {
  margin-bottom: var(--spacing-xs);
}

.hint {
  color: var(--text-muted);
  font-size: 0.875rem;
}

.query-input-container {
  margin-bottom: var(--spacing-lg);
}

.input-wrapper {
  display: flex;
  align-items: center;
  background: var(--bg-tertiary);
  border: 2px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-sm) var(--spacing-md);
  transition: all var(--transition-fast);
}

.input-wrapper:focus-within {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(0, 212, 255, 0.1);
}

.input-icon {
  color: var(--text-muted);
  flex-shrink: 0;
}

.query-input {
  flex: 1;
  background: transparent;
  border: none;
  padding: var(--spacing-sm) var(--spacing-md);
  color: var(--text-primary);
  font-family: inherit;
  font-size: 1rem;
}

.query-input:focus {
  outline: none;
}

.query-input::placeholder {
  color: var(--text-muted);
}

.submit-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  height: 44px;
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  border: none;
  border-radius: var(--radius-md);
  color: white;
  cursor: pointer;
  transition: all var(--transition-fast);
  flex-shrink: 0;
}

.submit-btn:hover:not(:disabled) {
  transform: scale(1.05);
  box-shadow: 0 0 20px rgba(0, 212, 255, 0.3);
}

.submit-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.examples {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-sm);
  align-items: center;
}

.examples-label {
  color: var(--text-muted);
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.example-btn {
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

.example-btn:hover {
  background: var(--bg-elevated);
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

.history-panel {
  grid-column: 2;
  grid-row: 1 / 3;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
  max-height: 400px;
  overflow-y: auto;
}

.history-panel h4 {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: var(--spacing-md);
}

.history-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.history-item {
  padding: var(--spacing-sm) var(--spacing-md);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.history-item:hover {
  background: var(--bg-elevated);
  border-color: var(--accent-primary);
}

.history-item.error {
  border-left: 3px solid var(--accent-error);
}

.history-question {
  font-size: 0.8125rem;
  color: var(--text-primary);
  margin-bottom: var(--spacing-xs);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.history-meta {
  display: flex;
  justify-content: space-between;
  font-size: 0.6875rem;
  color: var(--text-muted);
}

.status.success {
  color: var(--accent-success);
}

.status.error {
  color: var(--accent-error);
}

.tips-panel {
  grid-column: 1;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-lg);
}

.tips-panel h4 {
  font-size: 0.875rem;
  margin-bottom: var(--spacing-md);
}

.tips-panel ul {
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.tips-panel li {
  font-size: 0.8125rem;
  color: var(--text-secondary);
  padding-left: var(--spacing-md);
  position: relative;
}

.tips-panel li::before {
  content: '→';
  position: absolute;
  left: 0;
  color: var(--accent-primary);
}

.tips-panel strong {
  color: var(--accent-primary);
}
</style>

