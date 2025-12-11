# AI Pivot Studio

A modern AI-powered pivot analysis platform that supports Mondrian XML schemas and natural language queries.

## Intro Videos

- [AI Pivot Studio Demo](https://youtu.be/a7EvGdzJ3CQ)
- [Drill-Down/Up](https://youtu.be/Mz-cKirAxxw)
- [Process Performance Anaytics Example](https://youtu.be/UAy3BdgiViE)

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

## Screenshots

### 1. Cube Modeler - Star Schema Visualization

Visualize your data warehouse structure with an interactive star schema diagram. Dimension tables and fact tables are displayed with their relationships, making it easy to understand the cube structure at a glance.

![Cube Modeler - Star Schema](doc/ai%20olap%20studio1.png)

### 2. Pivot Analysis - Drag & Drop Interface

Build pivot reports effortlessly by dragging dimensions to Rows/Columns and measures to the Measures zone. Execute queries instantly and view results in a dynamic pivot table with drill-down capabilities.

![Pivot Analysis](doc/ai%20olap%20studio2.png)

### 3. AI Cube Generator

Describe your cube in natural language and let AI generate the complete Mondrian XML schema. Simply describe dimensions, hierarchies, and measures - the AI handles the rest.

![AI Cube Generator](doc/ai%20olap%20studio3.png)

### 4. Auto-Generate Database Tables & Sample Data

After creating a cube, automatically generate PostgreSQL DDL statements and realistic sample data. The AI creates table structures matching your cube definition and populates them with contextually appropriate test data.

![Create Database Tables](doc/ai%20olap%20studio4.png)

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

### 4. Docker Compose Stack

Create a `.env` file in the repository root with the production OpenAI credential you want to use:

```
OPENAI_API_KEY=sk-xxxx
```

The backend already falls back to the bundled PostgreSQL URL and GPT model defaults, so no other variables are required unless you want to customize them.

From the repository root, run the docker compose stack that builds and starts both services together:

```bash
docker compose up --build
```

The `frontend` service exposes the app on `http://localhost:5173` and talks to the `backend` API at `http://backend:8000/api`.

Stop the stack with `docker compose down` and use `docker compose up --build` when dependencies change.

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
| `OPENAI_API_KEY` | OpenAI API key (only required value for `docker compose up`; other settings fall back to defaults) | — |
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

