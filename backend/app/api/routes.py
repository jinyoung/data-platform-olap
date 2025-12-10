"""API routes for AI Pivot Studio."""
from typing import List, Optional
from fastapi import APIRouter, File, UploadFile, HTTPException
from pydantic import BaseModel

from ..models.cube import Cube, CubeMetadata
from ..models.query import PivotQuery, NaturalQuery, QueryResult
from ..services.xml_parser import MondrianXMLParser
from ..services.metadata_store import metadata_store
from ..services.sql_generator import SQLGenerator
from ..services.db_executor import db_executor
from ..langgraph_workflow.text2sql import get_workflow

router = APIRouter()


# ============== Schema Management ==============

@router.post("/schema/upload", response_model=CubeMetadata)
async def upload_schema(file: UploadFile = File(...)):
    """
    Upload a Mondrian XML schema file.
    Parses the XML and stores the metadata.
    """
    if not file.filename.endswith('.xml'):
        raise HTTPException(status_code=400, detail="File must be XML format")
    
    try:
        content = await file.read()
        xml_content = content.decode('utf-8')
        
        parser = MondrianXMLParser()
        metadata = parser.parse(xml_content)
        
        # Store in memory
        metadata_store.load_metadata(metadata)
        
        return metadata
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse XML: {str(e)}")


class SchemaTextInput(BaseModel):
    """Input for uploading schema as text."""
    xml_content: str


@router.post("/schema/upload-text", response_model=CubeMetadata)
async def upload_schema_text(input_data: SchemaTextInput):
    """
    Upload a Mondrian XML schema as text content.
    """
    try:
        parser = MondrianXMLParser()
        metadata = parser.parse(input_data.xml_content)
        metadata_store.load_metadata(metadata)
        return metadata
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse XML: {str(e)}")


# ============== Cube Information ==============

class CubeListResponse(BaseModel):
    """Response for cube list."""
    cubes: List[str]


@router.get("/cubes", response_model=CubeListResponse)
async def list_cubes():
    """Get list of all loaded cube names."""
    return CubeListResponse(cubes=metadata_store.get_cube_names())


@router.get("/cube/{name}/metadata", response_model=Cube)
async def get_cube_metadata(name: str):
    """Get metadata for a specific cube."""
    cube = metadata_store.get_cube(name)
    if not cube:
        raise HTTPException(status_code=404, detail=f"Cube '{name}' not found")
    return cube


@router.get("/cube/{name}/schema-description")
async def get_cube_schema_description(name: str):
    """Get human-readable schema description for a cube."""
    cube = metadata_store.get_cube(name)
    if not cube:
        raise HTTPException(status_code=404, detail=f"Cube '{name}' not found")
    
    description = metadata_store.get_schema_description(name)
    return {"description": description}


# ============== Pivot Query ==============

@router.post("/pivot/query", response_model=QueryResult)
async def execute_pivot_query(query: PivotQuery):
    """
    Execute a pivot query.
    Generates SQL from pivot configuration and executes it.
    """
    cube = metadata_store.get_cube(query.cube_name)
    if not cube:
        raise HTTPException(status_code=404, detail=f"Cube '{query.cube_name}' not found")
    
    try:
        generator = SQLGenerator(cube)
        sql = generator.generate_pivot_sql(query)
        
        result = await db_executor.execute_query(sql)
        return result
    except Exception as e:
        return QueryResult(sql="", error=str(e))


@router.post("/pivot/preview-sql")
async def preview_pivot_sql(query: PivotQuery):
    """
    Preview the SQL that would be generated for a pivot query.
    Does not execute the query.
    """
    cube = metadata_store.get_cube(query.cube_name)
    if not cube:
        raise HTTPException(status_code=404, detail=f"Cube '{query.cube_name}' not found")
    
    try:
        generator = SQLGenerator(cube)
        sql = generator.generate_pivot_sql(query)
        return {"sql": sql}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============== Natural Language Query ==============

class NL2SQLResponse(BaseModel):
    """Response for natural language to SQL conversion."""
    question: str
    sql: Optional[str] = None
    columns: List[str] = []
    rows: List[dict] = []
    row_count: int = 0
    execution_time_ms: float = 0.0
    error: Optional[str] = None


@router.post("/nl2sql", response_model=NL2SQLResponse)
async def natural_language_query(query: NaturalQuery):
    """
    Convert natural language question to SQL and execute it.
    Uses LangGraph workflow with LLM.
    """
    if not metadata_store.get_cube_names():
        raise HTTPException(
            status_code=400, 
            detail="No schema loaded. Please upload a Mondrian XML schema first."
        )
    
    try:
        workflow = get_workflow()
        
        initial_state = {
            "question": query.question,
            "cube_name": query.cube_name,
            "schema_description": "",
            "generated_sql": "",
            "validated_sql": "",
            "result": None,
            "error": None
        }
        
        final_state = await workflow.ainvoke(initial_state)
        
        if final_state.get("error"):
            return NL2SQLResponse(
                question=query.question,
                sql=final_state.get("validated_sql") or final_state.get("generated_sql"),
                error=final_state["error"]
            )
        
        result = final_state.get("result", {})
        return NL2SQLResponse(
            question=query.question,
            sql=result.get("sql", ""),
            columns=result.get("columns", []),
            rows=result.get("rows", []),
            row_count=result.get("row_count", 0),
            execution_time_ms=result.get("execution_time_ms", 0.0)
        )
    except Exception as e:
        return NL2SQLResponse(
            question=query.question,
            error=f"Workflow error: {str(e)}"
        )


@router.post("/nl2sql/preview")
async def preview_natural_language_sql(query: NaturalQuery):
    """
    Preview the SQL that would be generated from natural language.
    Does not execute the query.
    """
    if not metadata_store.get_cube_names():
        raise HTTPException(
            status_code=400,
            detail="No schema loaded. Please upload a Mondrian XML schema first."
        )
    
    try:
        workflow = get_workflow()
        
        # We'll run a modified workflow that stops after validation
        from ..langgraph_workflow.text2sql import Text2SQLWorkflow, Text2SQLState
        
        wf = Text2SQLWorkflow()
        state: Text2SQLState = {
            "question": query.question,
            "cube_name": query.cube_name,
            "schema_description": "",
            "generated_sql": "",
            "validated_sql": "",
            "result": None,
            "error": None
        }
        
        state = await wf.load_metadata(state)
        if state.get("error"):
            return {"sql": None, "error": state["error"]}
        
        state = await wf.generate_sql(state)
        if state.get("error"):
            return {"sql": None, "error": state["error"]}
        
        state = await wf.validate_sql(state)
        
        return {
            "sql": state.get("validated_sql") or state.get("generated_sql"),
            "error": state.get("error")
        }
    except Exception as e:
        return {"sql": None, "error": str(e)}


# ============== Cube Generation ==============

class CubeGenerateRequest(BaseModel):
    """Request for AI-based cube generation."""
    prompt: str


@router.post("/cube/generate")
async def generate_cube_from_prompt(request: CubeGenerateRequest):
    """
    Generate a Mondrian XML cube schema from natural language description.
    Uses LLM to create the cube structure.
    """
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage
    from ..core.config import get_settings
    
    settings = get_settings()
    
    system_prompt = """You are an expert in data warehouse modeling and Mondrian OLAP schemas.
Generate a valid Mondrian XML cube schema based on the user's description.

RULES:
1. Generate valid Mondrian XML 3.x schema format
2. Include proper Schema, Cube, Table, Dimension, Hierarchy, Level, and Measure elements
3. Use appropriate foreignKey attributes for dimensions
4. Use primaryKey="id" for hierarchies unless specified otherwise
5. Include appropriate aggregators for measures (sum, count, avg, min, max, distinct-count)
6. Add formatString attributes for measures
7. Use snake_case for table and column names
8. Use CamelCase for dimension and measure names

OUTPUT FORMAT:
Return ONLY the XML content, starting with <?xml version="1.0" encoding="UTF-8"?>
Do not include any explanations or markdown code blocks.

EXAMPLE OUTPUT:
<?xml version="1.0" encoding="UTF-8"?>
<Schema name="ExampleSchema">
  <Cube name="ExampleCube">
    <Table name="fact_example"/>
    <Dimension name="TimeDim" foreignKey="time_id">
      <Hierarchy hasAll="true" primaryKey="id">
        <Table name="dim_time"/>
        <Level name="Year" column="year"/>
        <Level name="Month" column="month"/>
      </Hierarchy>
    </Dimension>
    <Measure name="Amount" column="amount" aggregator="sum" formatString="#,###"/>
  </Cube>
</Schema>"""

    try:
        llm = ChatOpenAI(
            model=settings.openai_model,
            api_key=settings.openai_api_key,
            temperature=0.2
        )
        
        response = await llm.ainvoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Create a Mondrian cube schema for: {request.prompt}")
        ])
        
        xml_content = response.content.strip()
        
        # Clean up any markdown formatting
        if xml_content.startswith("```"):
            lines = xml_content.split("\n")
            xml_content = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        
        # Validate it's proper XML
        if not xml_content.startswith("<?xml"):
            return {"xml": None, "error": "Generated content is not valid XML"}
        
        return {"xml": xml_content, "error": None}
        
    except Exception as e:
        return {"xml": None, "error": str(e)}


# ============== Table & Sample Data Generation ==============

class GenerateTablesRequest(BaseModel):
    """Request for generating table DDL and sample data."""
    cube_name: str
    sample_rows: int = 100


@router.post("/cube/{cube_name}/generate-tables")
async def generate_table_ddl(cube_name: str, sample_rows: int = 100):
    """
    Generate SQL DDL statements to create tables for a cube.
    Also generates sample data INSERT statements.
    """
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage
    from ..core.config import get_settings
    
    cube = metadata_store.get_cube(cube_name)
    if not cube:
        raise HTTPException(status_code=404, detail=f"Cube '{cube_name}' not found")
    
    settings = get_settings()
    schema_desc = metadata_store.get_schema_description(cube_name)
    
    system_prompt = f"""You are an expert PostgreSQL database administrator.
Generate SQL statements to create the tables and sample data for an OLAP cube.

RULES:
1. Generate CREATE TABLE statements with appropriate data types
2. Use SERIAL PRIMARY KEY for id columns
3. Add FOREIGN KEY constraints on fact tables referencing dimension tables
4. Generate realistic sample data that makes sense for the business context
5. Generate exactly {sample_rows} rows for the fact table
6. Generate appropriate dimension data (10-50 rows per dimension)
7. Ensure referential integrity in sample data
8. Use Korean or English data depending on the context
9. Include CREATE INDEX statements for foreign keys

OUTPUT FORMAT:
Return ONLY valid PostgreSQL SQL statements.
Start with DROP TABLE IF EXISTS statements (in correct order for FK constraints).
Then CREATE TABLE statements for dimensions first, then fact table.
Then INSERT statements for dimensions first, then fact table.
Do not include any explanations or markdown code blocks.
"""

    try:
        llm = ChatOpenAI(
            model=settings.openai_model,
            api_key=settings.openai_api_key,
            temperature=0.3
        )
        
        response = await llm.ainvoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"""Generate PostgreSQL DDL and sample data for this cube:

{schema_desc}

Generate {sample_rows} rows of sample fact data with realistic values.""")
        ])
        
        sql_content = response.content.strip()
        
        # Clean up any markdown formatting
        if sql_content.startswith("```"):
            lines = sql_content.split("\n")
            sql_content = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        
        return {"sql": sql_content, "error": None}
        
    except Exception as e:
        return {"sql": None, "error": str(e)}


class ExecuteSQLRequest(BaseModel):
    """Request for executing SQL statements."""
    sql: str


@router.post("/cube/execute-sql")
async def execute_sql_statements(request: ExecuteSQLRequest):
    """
    Execute SQL statements against the database.
    Used for creating tables and inserting sample data.
    """
    import asyncpg
    from ..core.config import get_settings
    
    settings = get_settings()
    
    try:
        # Connect to database
        conn = await asyncpg.connect(settings.database_url)
        
        try:
            # Split SQL into individual statements and execute
            # Handle multi-statement execution
            statements = []
            current_stmt = []
            in_function = False
            
            for line in request.sql.split('\n'):
                stripped = line.strip()
                
                # Track if we're inside a function/DO block
                if stripped.upper().startswith('DO $$') or stripped.upper().startswith('CREATE FUNCTION'):
                    in_function = True
                if in_function and stripped == '$$;':
                    in_function = False
                    current_stmt.append(line)
                    statements.append('\n'.join(current_stmt))
                    current_stmt = []
                    continue
                
                current_stmt.append(line)
                
                # If not in function and line ends with semicolon, end statement
                if not in_function and stripped.endswith(';'):
                    stmt = '\n'.join(current_stmt).strip()
                    if stmt and stmt != ';':
                        statements.append(stmt)
                    current_stmt = []
            
            # Execute each statement
            results = []
            for stmt in statements:
                if stmt.strip():
                    try:
                        await conn.execute(stmt)
                        results.append({"statement": stmt[:100] + "..." if len(stmt) > 100 else stmt, "status": "success"})
                    except Exception as stmt_error:
                        results.append({"statement": stmt[:100] + "..." if len(stmt) > 100 else stmt, "status": "error", "error": str(stmt_error)})
            
            return {
                "success": True,
                "statements_executed": len([r for r in results if r["status"] == "success"]),
                "statements_failed": len([r for r in results if r["status"] == "error"]),
                "details": results
            }
            
        finally:
            await conn.close()
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============== Health Check ==============

@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "cubes_loaded": len(metadata_store.get_cube_names())
    }

