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


# ============== DW Tables Management ==============

@router.get("/dw/tables")
async def list_dw_tables(schema_name: str = "dw"):
    """List all tables in the DW schema."""
    import asyncpg
    from ..core.config import get_settings
    
    settings = get_settings()
    
    try:
        conn = await asyncpg.connect(settings.database_url)
        try:
            # Get all tables in the dw schema
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = $1
                ORDER BY table_name
            """, schema_name)
            
            return {
                "schema": schema_name,
                "tables": [row["table_name"] for row in tables]
            }
        finally:
            await conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")


class DeleteDWTablesRequest(BaseModel):
    """Request to delete DW tables."""
    tables: List[str] = []  # Empty means delete all
    schema_name: str = "dw"


@router.post("/dw/tables/delete")
async def delete_dw_tables(request: DeleteDWTablesRequest):
    """Delete specified tables from the DW schema."""
    import asyncpg
    from ..core.config import get_settings
    
    settings = get_settings()
    
    try:
        conn = await asyncpg.connect(settings.database_url)
        try:
            deleted = []
            errors = []
            
            # If no specific tables, get all tables in the schema
            if not request.tables:
                tables = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = $1
                """, request.schema_name)
                table_names = [row["table_name"] for row in tables]
            else:
                table_names = request.tables
            
            # Delete each table with CASCADE
            for table in table_names:
                try:
                    await conn.execute(f'DROP TABLE IF EXISTS "{request.schema_name}"."{table}" CASCADE')
                    deleted.append(table)
                except Exception as e:
                    errors.append({"table": table, "error": str(e)})
            
            return {
                "success": True,
                "deleted": deleted,
                "errors": errors,
                "schema": request.schema_name
            }
        finally:
            await conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete tables: {str(e)}")


@router.delete("/dw/schema")
async def drop_dw_schema(schema_name: str = "dw"):
    """Drop the entire DW schema and all its tables."""
    import asyncpg
    from ..core.config import get_settings
    
    settings = get_settings()
    
    try:
        conn = await asyncpg.connect(settings.database_url)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            return {
                "success": True,
                "message": f"Schema '{schema_name}' dropped"
            }
        finally:
            await conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop schema: {str(e)}")


# ============== Data Lineage Overview ==============

@router.get("/lineage/overview")
async def get_lineage_overview():
    """Get data lineage overview for visualization.
    
    Returns all ETL configurations formatted for lineage diagram:
    - Source tables (OLTP)
    - ETL processes (cube configurations)
    - Target tables (OLAP star schema)
    - Data flow connections
    """
    try:
        configs = etl_service.get_all_etl_configs()
        
        # Collect unique source tables
        source_tables = []
        source_table_set = set()
        
        # Collect ETL processes (one per cube)
        etl_processes = []
        
        # Collect target tables (fact + dimensions)
        target_tables = []
        target_table_set = set()
        
        # Collect data flows
        data_flows = []
        
        for cube_name, config in configs.items():
            # Source tables
            for source in config.source_tables:
                if source not in source_table_set:
                    source_table_set.add(source)
                    # Count columns from mappings
                    col_count = len([m for m in config.mappings if m.source_table == source])
                    source_tables.append({
                        "id": f"src_{source}",
                        "name": source,
                        "type": "source",
                        "columns": col_count or 5,  # default
                        "schema": "public"
                    })
            
            # ETL Process for this cube
            process_id = f"etl_{cube_name}"
            etl_processes.append({
                "id": process_id,
                "name": f"ETL_{cube_name.upper()}",
                "cube_name": cube_name,
                "operation": "INSERT" if config.sync_mode == "full" else "MERGE",
                "sync_mode": config.sync_mode,
                "mappings_count": len(config.mappings)
            })
            
            # Target tables - Fact table
            fact_name = config.fact_table.split('.')[-1] if '.' in config.fact_table else config.fact_table
            if fact_name not in target_table_set:
                target_table_set.add(fact_name)
                fact_cols = len([m for m in config.mappings if m.target_table == config.fact_table or m.target_table == fact_name])
                target_tables.append({
                    "id": f"tgt_{fact_name}",
                    "name": fact_name,
                    "type": "fact",
                    "columns": fact_cols or 10,
                    "schema": config.dw_schema,
                    "cube_name": cube_name
                })
            
            # Target tables - Dimension tables
            for dim_table in config.dimension_tables:
                dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
                if dim_name not in target_table_set:
                    target_table_set.add(dim_name)
                    dim_cols = len([m for m in config.mappings if m.target_table == dim_table or m.target_table == dim_name])
                    target_tables.append({
                        "id": f"tgt_{dim_name}",
                        "name": dim_name,
                        "type": "dimension",
                        "columns": dim_cols or 5,
                        "schema": config.dw_schema,
                        "cube_name": cube_name
                    })
            
            # Data flows: source -> ETL
            for source in config.source_tables:
                data_flows.append({
                    "from": f"src_{source}",
                    "to": process_id,
                    "type": "extract"
                })
            
            # Data flows: ETL -> targets
            data_flows.append({
                "from": process_id,
                "to": f"tgt_{fact_name}",
                "type": "load"
            })
            for dim_table in config.dimension_tables:
                dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
                data_flows.append({
                    "from": process_id,
                    "to": f"tgt_{dim_name}",
                    "type": "load"
                })
        
        return {
            "source_tables": source_tables,
            "etl_processes": etl_processes,
            "target_tables": target_tables,
            "data_flows": data_flows,
            "summary": {
                "total_sources": len(source_tables),
                "total_etl_processes": len(etl_processes),
                "total_targets": len(target_tables),
                "total_flows": len(data_flows)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get lineage overview: {str(e)}")


@router.get("/lineage/{cube_name}")
async def get_cube_lineage(cube_name: str):
    """Get detailed lineage for a specific cube."""
    config = etl_service.get_etl_config(cube_name)
    if not config:
        raise HTTPException(status_code=404, detail=f"No ETL config found for cube: {cube_name}")
    
    # Build detailed lineage info
    source_columns = {}
    target_columns = {}
    
    for mapping in config.mappings:
        # Group by source table
        if mapping.source_table not in source_columns:
            source_columns[mapping.source_table] = []
        source_columns[mapping.source_table].append({
            "column": mapping.source_column,
            "target": f"{mapping.target_table}.{mapping.target_column}",
            "transformation": mapping.transformation
        })
        
        # Group by target table
        if mapping.target_table not in target_columns:
            target_columns[mapping.target_table] = []
        target_columns[mapping.target_table].append({
            "column": mapping.target_column,
            "source": f"{mapping.source_table}.{mapping.source_column}",
            "transformation": mapping.transformation
        })
    
    return {
        "cube_name": cube_name,
        "fact_table": config.fact_table,
        "dimension_tables": config.dimension_tables,
        "source_tables": config.source_tables,
        "sync_mode": config.sync_mode,
        "incremental_column": config.incremental_column,
        "source_columns": source_columns,
        "target_columns": target_columns,
        "created_at": config.created_at,
        "last_sync": config.last_sync
    }


# ============== Full Provisioning ==============

class ProvisionRequest(BaseModel):
    """Request for full cube provisioning."""
    cube_name: str
    fact_table: str
    dimensions: List[Dict[str, Any]]
    measures: List[Dict[str, Any]]
    dw_schema: str = "dw"
    generate_sample_data: bool = True


@router.post("/provision")
async def provision_cube(request: ProvisionRequest):
    """
    Full cube provisioning:
    1. Create DW schema
    2. Generate and execute DDL for all tables
    3. Populate dim_time with generated data
    4. Populate other dimensions from source tables (if available)
    """
    results = {
        "success": True,
        "steps": [],
        "errors": []
    }
    
    try:
        # Step 1: Create DW schema
        await etl_service.create_dw_schema(request.dw_schema)
        results["steps"].append({"step": "create_schema", "status": "success"})
        
        # Step 2: Build and execute DDL
        # Drop existing tables first
        drop_statements = []
        for dim in request.dimensions:
            dim_name = dim.get("name", "").replace(".", "_")
            drop_statements.append(f"DROP TABLE IF EXISTS {request.dw_schema}.{dim_name} CASCADE")
        
        fact_name = request.fact_table.split(".")[-1] if "." in request.fact_table else request.fact_table
        drop_statements.append(f"DROP TABLE IF EXISTS {request.dw_schema}.{fact_name} CASCADE")
        
        # Execute drop statements
        pool = await etl_service.get_pool()
        async with pool.acquire() as conn:
            for stmt in drop_statements:
                try:
                    await conn.execute(stmt)
                except Exception as e:
                    results["errors"].append(f"Drop: {str(e)}")
        
        # Build CREATE TABLE statements
        ddl_statements = []
        
        # Dimension tables
        for dim in request.dimensions:
            dim_name = dim.get("name", "dim_unknown")
            levels = dim.get("levels", [])
            
            columns = ["id SERIAL PRIMARY KEY"]
            
            # Special handling for dim_time - add date column and proper types
            if dim_name.lower() == "dim_time":
                columns.append("date DATE")
                columns.append("year INTEGER")
                columns.append("quarter INTEGER")
                columns.append("month INTEGER")
                columns.append("day INTEGER")
            else:
                for level in levels:
                    col_name = level.get("column", level.get("name", "value"))
                    columns.append(f"{col_name} VARCHAR(255)")
            
            columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
            
            ddl_statements.append(
                f"CREATE TABLE IF NOT EXISTS {request.dw_schema}.{dim_name} ({', '.join(columns)})"
            )
        
        # Fact table
        fact_columns = ["id SERIAL PRIMARY KEY"]
        for dim in request.dimensions:
            dim_name = dim.get("name", "dim_unknown")
            fact_columns.append(f"{dim_name}_id INTEGER")
        for measure in request.measures:
            col_name = measure.get("column", measure.get("name", "value"))
            fact_columns.append(f"{col_name} NUMERIC(15,4)")
        fact_columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
        
        ddl_statements.append(
            f"CREATE TABLE IF NOT EXISTS {request.dw_schema}.{fact_name} ({', '.join(fact_columns)})"
        )
        
        # Execute DDL
        async with pool.acquire() as conn:
            for stmt in ddl_statements:
                try:
                    await conn.execute(stmt)
                except Exception as e:
                    results["errors"].append(f"DDL: {str(e)}")
        
        results["steps"].append({"step": "create_tables", "status": "success", "tables": len(ddl_statements)})
        
        # Step 3: Populate dim_time if it exists
        if request.generate_sample_data:
            has_dim_time = any(d.get("name", "").lower() == "dim_time" for d in request.dimensions)
            if has_dim_time:
                try:
                    time_sql = f"""
                    INSERT INTO {request.dw_schema}.dim_time (date, year, quarter, month, day)
                    SELECT 
                        d::date,
                        EXTRACT(YEAR FROM d)::int,
                        EXTRACT(QUARTER FROM d)::int,
                        EXTRACT(MONTH FROM d)::int,
                        EXTRACT(DAY FROM d)::int
                    FROM generate_series(
                        CURRENT_DATE - INTERVAL '365 days',
                        CURRENT_DATE,
                        '1 day'::interval
                    ) d
                    ON CONFLICT DO NOTHING
                    """
                    async with pool.acquire() as conn:
                        await conn.execute(time_sql)
                    results["steps"].append({"step": "populate_dim_time", "status": "success"})
                except Exception as e:
                    results["errors"].append(f"dim_time: {str(e)}")
        
        results["cube_name"] = request.cube_name
        results["tables_created"] = [d.get("name") for d in request.dimensions] + [fact_name]
        
    except Exception as e:
        results["success"] = False
        results["errors"].append(str(e))
    
    return results


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

