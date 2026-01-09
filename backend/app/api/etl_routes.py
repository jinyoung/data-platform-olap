"""ETL API routes for data pipeline management.

Endpoints:
- GET  /api/etl/catalog          : Explore source tables from Neo4j catalog
- GET  /api/etl/catalog/{table}  : Get table details with columns
- POST /api/etl/suggest          : AI-suggested ETL strategy
- POST /api/etl/config           : Create ETL configuration
- GET  /api/etl/config/{cube}    : Get ETL configuration
- POST /api/etl/schema/create    : Create DW schema
- POST /api/etl/ddl/generate     : Generate star schema DDL
- POST /api/etl/ddl/execute      : Execute DDL statements
- POST /api/etl/sync/{cube}      : Execute ETL sync
- POST /api/etl/lineage/{cube}   : Register lineage in Neo4j
"""
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..services.etl_service import etl_service, ETLStatus
from ..services.neo4j_client import neo4j_client

router = APIRouter(prefix="/etl", tags=["ETL"])


# ============== Request/Response Models ==============

class CatalogQuery(BaseModel):
    """Query parameters for catalog exploration."""
    user_id: Optional[str] = None
    project_name: Optional[str] = None
    schema: Optional[str] = None
    search: Optional[str] = None


class ETLSuggestRequest(BaseModel):
    """Request for AI ETL suggestion."""
    cube_description: str
    user_id: Optional[str] = None
    project_name: Optional[str] = None


class ETLMappingInput(BaseModel):
    """ETL column mapping input."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transformation: str = ""


class ETLConfigRequest(BaseModel):
    """Request for creating ETL configuration."""
    cube_name: str
    fact_table: str
    dimension_tables: List[str]
    source_tables: List[str]
    mappings: List[ETLMappingInput]
    dw_schema: str = "dw"
    sync_mode: str = "full"
    incremental_column: Optional[str] = None


class DimensionInput(BaseModel):
    """Dimension table definition."""
    name: str
    table_name: str
    columns: List[Dict[str, str]]


class ColumnInput(BaseModel):
    """Column definition."""
    name: str
    dtype: str = "VARCHAR(255)"
    description: str = ""


class StarSchemaDDLRequest(BaseModel):
    """Request for generating star schema DDL."""
    cube_name: str
    fact_table_name: str
    fact_columns: List[ColumnInput]
    dimensions: List[DimensionInput]
    dw_schema: str = "dw"


class ExecuteDDLRequest(BaseModel):
    """Request for executing DDL."""
    ddl: str


class SyncRequest(BaseModel):
    """Request for ETL sync."""
    force_full: bool = False


class LineageRequest(BaseModel):
    """Request for lineage registration."""
    user_id: str
    project_name: str


# ============== Catalog Exploration ==============

@router.get("/catalog")
async def explore_catalog(
    user_id: Optional[str] = Query(None),
    project_name: Optional[str] = Query(None),
    schema: Optional[str] = Query(None),
    search: Optional[str] = Query(None)
):
    """Explore source tables from Neo4j catalog.
    
    Returns tables with columns and relationships.
    """
    try:
        result = await etl_service.explore_source_catalog(
            user_id=user_id,
            project_name=project_name,
            schema=schema,
            search=search
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to explore catalog: {str(e)}")


@router.get("/catalog/{table_name}")
async def get_table_details(
    table_name: str,
    schema: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    project_name: Optional[str] = Query(None)
):
    """Get detailed information about a specific table."""
    try:
        result = await etl_service.get_table_details(
            table_name=table_name,
            schema=schema,
            user_id=user_id,
            project_name=project_name
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get table details: {str(e)}")


# ============== ETL Configuration ==============

@router.post("/suggest")
async def suggest_etl_strategy(request: ETLSuggestRequest):
    """Get AI-suggested ETL strategy based on cube description.
    
    Uses LLM to analyze available tables and suggest mappings.
    """
    try:
        # First get available tables
        catalog = await etl_service.explore_source_catalog(
            user_id=request.user_id,
            project_name=request.project_name
        )
        
        if not catalog.get("tables"):
            return {
                "error": "No source tables found in catalog",
                "suggestion": None
            }
        
        # Get AI suggestion
        suggestion = await etl_service.suggest_etl_strategy(
            cube_description=request.cube_description,
            available_tables=catalog["tables"]
        )
        
        return {
            "suggestion": suggestion,
            "available_tables": len(catalog["tables"])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to suggest ETL strategy: {str(e)}")


@router.post("/config")
async def create_etl_config(request: ETLConfigRequest):
    """Create ETL configuration for a cube."""
    try:
        config = await etl_service.create_etl_config(
            cube_name=request.cube_name,
            fact_table=request.fact_table,
            dimension_tables=request.dimension_tables,
            source_tables=request.source_tables,
            mappings=[m.model_dump() for m in request.mappings],
            dw_schema=request.dw_schema,
            sync_mode=request.sync_mode,
            incremental_column=request.incremental_column
        )
        
        return {
            "success": True,
            "config": config.to_dict()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create ETL config: {str(e)}")


@router.get("/config/{cube_name}")
async def get_etl_config(cube_name: str):
    """Get ETL configuration for a cube."""
    config = etl_service.get_etl_config(cube_name)
    
    if not config:
        raise HTTPException(status_code=404, detail=f"No ETL config found for cube: {cube_name}")
    
    return config.to_dict()


@router.delete("/config/{cube_name}")
async def delete_etl_config(cube_name: str):
    """Delete ETL configuration for a cube."""
    success = etl_service.delete_etl_config(cube_name)
    if not success:
        raise HTTPException(status_code=404, detail=f"No ETL config found for cube: {cube_name}")
    return {"success": True, "message": f"ETL config for '{cube_name}' deleted"}


@router.get("/configs")
async def list_etl_configs():
    """List all ETL configurations."""
    configs = etl_service.get_all_etl_configs()
    return {
        "configs": [{"cube_name": name, **config.to_dict()} for name, config in configs.items()]
    }


@router.delete("/configs")
async def delete_all_etl_configs():
    """Delete all ETL configurations."""
    etl_service.clear_all_etl_configs()
    return {"success": True, "message": "All ETL configs deleted"}


# ============== DW Schema Management ==============

@router.post("/schema/create")
async def create_dw_schema(schema_name: str = "dw"):
    """Create the DW schema in PostgreSQL."""
    try:
        result = await etl_service.create_dw_schema(schema_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create schema: {str(e)}")


@router.post("/ddl/generate")
async def generate_star_schema_ddl(request: StarSchemaDDLRequest):
    """Generate DDL for star schema tables."""
    try:
        ddl = await etl_service.generate_star_schema_ddl(
            cube_name=request.cube_name,
            fact_table_name=request.fact_table_name,
            fact_columns=[c.model_dump() for c in request.fact_columns],
            dimensions=[d.model_dump() for d in request.dimensions],
            dw_schema=request.dw_schema
        )
        
        return {
            "ddl": ddl,
            "cube_name": request.cube_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate DDL: {str(e)}")


@router.post("/ddl/execute")
async def execute_ddl(request: ExecuteDDLRequest):
    """Execute DDL statements."""
    try:
        result = await etl_service.execute_ddl(request.ddl)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute DDL: {str(e)}")


# ============== ETL Sync ==============

@router.post("/sync/{cube_name}")
async def sync_data(cube_name: str, request: SyncRequest = None):
    """Execute ETL sync for a cube.
    
    Syncs data from OLTP source tables to OLAP star schema.
    """
    try:
        force_full = request.force_full if request else False
        result = await etl_service.sync_data(
            cube_name=cube_name,
            force_full=force_full
        )
        
        return {
            "status": result.status.value,
            "rows_inserted": result.rows_inserted,
            "rows_updated": result.rows_updated,
            "duration_ms": result.duration_ms,
            "error": result.error,
            "details": result.details
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to sync data: {str(e)}")


# ============== Lineage Registration ==============

@router.post("/lineage/{cube_name}")
async def register_lineage(cube_name: str, request: LineageRequest):
    """Register OLAP tables and lineage in Neo4j.
    
    Creates Table nodes for OLAP tables and DATA_FLOW_TO relationships
    to source tables for lineage tracking.
    """
    try:
        result = await etl_service.register_lineage(
            cube_name=cube_name,
            user_id=request.user_id,
            project_name=request.project_name
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register lineage: {str(e)}")


# ============== Health Check ==============

@router.get("/health")
async def etl_health():
    """ETL service health check."""
    # Try to connect to Neo4j
    neo4j_status = "unknown"
    try:
        async with neo4j_client:
            await neo4j_client.execute_query("RETURN 1 as test")
            neo4j_status = "connected"
    except Exception as e:
        neo4j_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "neo4j": neo4j_status,
        "configs_loaded": len(etl_service._configs)
    }

