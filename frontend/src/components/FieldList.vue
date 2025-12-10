<script setup>
import { useCubeStore } from '../store/cubeStore'
import draggable from 'vuedraggable'

const store = useCubeStore()

const emit = defineEmits(['dragStart'])

const handleDragStart = (item, type) => {
  emit('dragStart', { item, type })
}
</script>

<template>
  <div class="field-list">
    <!-- Dimensions -->
    <div class="field-section">
      <h4 class="section-title">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="3" y="3" width="18" height="18" rx="2"/>
          <path d="M3 9h18"/>
          <path d="M9 21V9"/>
        </svg>
        Dimensions
      </h4>
      
      <div class="field-items">
        <div 
          v-for="dim in store.dimensions" 
          :key="dim.name"
          class="dimension-group"
        >
          <div class="dim-header">{{ dim.name }}</div>
          <draggable
            :list="dim.levels"
            :group="{ name: 'fields', pull: 'clone', put: false }"
            item-key="name"
            :clone="item => ({ dimension: dim.name, level: item.name })"
            @start="handleDragStart($event, 'dimension')"
            class="level-list"
          >
            <template #item="{ element }">
              <div class="tag dimension" draggable="true">
                <span class="level-icon">↳</span>
                {{ element.name }}
              </div>
            </template>
          </draggable>
        </div>
      </div>
    </div>
    
    <!-- Measures -->
    <div class="field-section">
      <h4 class="section-title">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <line x1="12" y1="20" x2="12" y2="10"/>
          <line x1="18" y1="20" x2="18" y2="4"/>
          <line x1="6" y1="20" x2="6" y2="16"/>
        </svg>
        Measures
      </h4>
      
      <draggable
        :list="store.measures"
        :group="{ name: 'measures', pull: 'clone', put: false }"
        item-key="name"
        :clone="item => ({ name: item.name })"
        class="field-items"
      >
        <template #item="{ element }">
          <div class="tag measure" draggable="true">
            <span class="measure-icon">Σ</span>
            {{ element.name }}
            <span class="agg-badge">{{ element.agg }}</span>
          </div>
        </template>
      </draggable>
    </div>
  </div>
</template>

<style scoped>
.field-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-lg);
}

.field-section {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: var(--spacing-md);
}

.section-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: var(--spacing-md);
  padding-bottom: var(--spacing-sm);
  border-bottom: 1px solid var(--border-color);
}

.field-items {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.dimension-group {
  margin-bottom: var(--spacing-sm);
}

.dim-header {
  font-size: 0.8125rem;
  font-weight: 500;
  color: var(--accent-secondary);
  margin-bottom: var(--spacing-xs);
  padding-left: var(--spacing-xs);
}

.level-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-xs);
  padding-left: var(--spacing-sm);
}

.level-icon {
  color: var(--text-muted);
  font-size: 0.75rem;
}

.measure-icon {
  color: var(--accent-success);
  font-weight: 600;
}

.agg-badge {
  font-size: 0.625rem;
  padding: 1px 4px;
  background: var(--bg-primary);
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  text-transform: uppercase;
}

.tag {
  cursor: grab;
}

.tag:active {
  cursor: grabbing;
}
</style>

