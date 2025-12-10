# AI Pivot Studio

A modern AI-powered pivot analysis platform that supports Mondrian XML schemas and natural language queries.

## Intro Videos

- [AI Pivot Studio Demo](https://youtu.be/a7EvGdzJ3CQ)
- [Pivot Table](https://youtu.be/Mz-cKirAxxw)
- [Drill-Down/Up](https://youtu.be/UAy3BdgiViE)

![Vue.js](https://img.shields.io/badge/Vue.js-3.x-4FC08D?logo=vue.js)
![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python)
![LangChain](https://img.shields.io/badge/LangChain-LangGraph-1C3C3C)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?logo=postgresql)

## Features

- 📊 **Pivot Analysis**: Drag & drop interface for building pivot reports
- 💬 **Natural Language Queries**: Ask questions in plain English, get SQL results
- 📁 **Mondrian XML Support**: Import existing Mondrian schema files
- 🤖 **AI-Powered**: Uses OpenAI GPT models for Text2SQL conversion
- ⚡ **Fast & Modern**: Vue.js 3 frontend with a beautiful dark theme

## Architecture

```
┌─────────────────┐     ┌──────────────────────┐     ┌────────────┐
│   Vue.js SPA    │────▶│  Python Backend      │────▶│ PostgreSQL │
│  (Pivot UI)     │     │  (FastAPI + LangGraph)│     │    (DW)    │
└─────────────────┘     └──────────────────────┘     └────────────┘
                               │
                               ▼
                        ┌──────────────┐
                        │   OpenAI     │
                        │   GPT API    │
                        └──────────────┘
```

## Prerequisites

- Python 3.11+
- Node.js 18+
- PostgreSQL 15+
- OpenAI API Key

## Quick Start

### 1. Backend Setup

```bash
cd backend

# Create virtual environment with uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .

# Set environment variables (or create .env file)
export OPENAI_API_KEY="your-openai-api-key"
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/pivot_studio"

# Run the server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev
```

### 3. Access the Application

- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## Usage

### 1. Upload a Schema

Upload a Mondrian XML schema file or use the sample schema provided in the UI.

Example Mondrian XML structure:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Schema name="SalesAnalysis">
  <Cube name="Sales">
    <Table name="fact_sales"/>
    
    <Dimension name="Date" foreignKey="date_id">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="dim_date"/>
        <Level name="Year" column="year"/>
        <Level name="Month" column="month"/>
      </Hierarchy>
    </Dimension>
    
    <Measure name="SalesAmount" column="sales_amount" aggregator="sum"/>
  </Cube>
</Schema>
```

### 2. Pivot Analysis

1. Select a cube from the sidebar
2. Drag dimensions to Rows or Columns
3. Drag measures to the Measures zone
4. Click Execute to run the query

### 3. Natural Language Queries

Switch to the "Natural Language" tab and ask questions like:
- "Show me total sales by year"
- "What are the top 10 products by revenue?"
- "Monthly sales trend for 2024"

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/schema/upload` | POST | Upload Mondrian XML file |
| `/api/schema/upload-text` | POST | Upload XML as text |
| `/api/cubes` | GET | List all cubes |
| `/api/cube/{name}/metadata` | GET | Get cube metadata |
| `/api/pivot/query` | POST | Execute pivot query |
| `/api/nl2sql` | POST | Natural language query |
| `/api/health` | GET | Health check |

## Project Structure

```
langolap/
├── backend/
│   ├── app/
│   │   ├── api/           # REST API routes
│   │   ├── core/          # Configuration
│   │   ├── models/        # Pydantic models
│   │   ├── services/      # Business logic
│   │   └── langgraph_workflow/  # Text2SQL workflow
│   └── pyproject.toml
│
├── frontend/
│   ├── src/
│   │   ├── components/    # Vue components
│   │   ├── store/         # Pinia store
│   │   ├── services/      # API client
│   │   └── assets/        # Styles
│   └── package.json
│
└── README.md
```

## Configuration

### Backend Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENAI_API_KEY` | OpenAI API key | Required |
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://postgres:postgres@localhost:5432/pivot_studio` |
| `OPENAI_MODEL` | OpenAI model to use | `gpt-4o-mini` |

## Development

### Running Tests

```bash
# Backend
cd backend
pytest

# Frontend
cd frontend
npm run test
```

### Building for Production

```bash
# Frontend
cd frontend
npm run build

# The built files will be in frontend/dist/
```

## License

MIT License

