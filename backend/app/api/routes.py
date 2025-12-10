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


# ============== Health Check ==============

@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "cubes_loaded": len(metadata_store.get_cube_names())
    }

