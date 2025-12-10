import axios from 'axios'

const API_BASE_URL = 'http://localhost:8000/api'

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  }
})

// Schema Management
export const uploadSchema = async (file) => {
  const formData = new FormData()
  formData.append('file', file)
  const response = await api.post('/schema/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  })
  return response.data
}

export const uploadSchemaText = async (xmlContent) => {
  const response = await api.post('/schema/upload-text', { xml_content: xmlContent })
  return response.data
}

// Cube Information
export const getCubes = async () => {
  const response = await api.get('/cubes')
  return response.data
}

export const getCubeMetadata = async (cubeName) => {
  const response = await api.get(`/cube/${cubeName}/metadata`)
  return response.data
}

export const getCubeSchemaDescription = async (cubeName) => {
  const response = await api.get(`/cube/${cubeName}/schema-description`)
  return response.data
}

// Pivot Query
export const executePivotQuery = async (query) => {
  const response = await api.post('/pivot/query', query)
  return response.data
}

export const previewPivotSQL = async (query) => {
  const response = await api.post('/pivot/preview-sql', query)
  return response.data
}

// Natural Language Query
export const executeNL2SQL = async (question, cubeName = null) => {
  const response = await api.post('/nl2sql', {
    question,
    cube_name: cubeName
  })
  return response.data
}

export const previewNL2SQL = async (question, cubeName = null) => {
  const response = await api.post('/nl2sql/preview', {
    question,
    cube_name: cubeName
  })
  return response.data
}

// Health Check
export const healthCheck = async () => {
  const response = await api.get('/health')
  return response.data
}

export default api

