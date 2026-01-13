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
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..services.etl_service import etl_service, ETLStatus
from ..services.neo4j_client import neo4j_client
from ..services.robo_analyzer_client import (
    robo_analyzer_client, 
    DWColumnInfo, 
    DWDimensionInfo, 
    DWFactTableInfo
)

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
        
        # Step 4: Register star schema in Neo4j
        try:
            # Prepare dimension data for Neo4j
            neo4j_dimensions = []
            for dim in request.dimensions:
                dim_name = dim.get("name", "dim_unknown")
                levels = dim.get("levels", [])
                
                dim_columns = []
                if dim_name.lower() == "dim_time":
                    dim_columns = [
                        {"name": "date", "dtype": "DATE", "description": "날짜"},
                        {"name": "year", "dtype": "INTEGER", "description": "연도"},
                        {"name": "quarter", "dtype": "INTEGER", "description": "분기"},
                        {"name": "month", "dtype": "INTEGER", "description": "월"},
                        {"name": "day", "dtype": "INTEGER", "description": "일"}
                    ]
                else:
                    for level in levels:
                        col_name = level.get("column", level.get("name", "value"))
                        dim_columns.append({
                            "name": col_name,
                            "dtype": "VARCHAR(255)",
                            "description": level.get("description", "")
                        })
                
                neo4j_dimensions.append({
                    "name": dim_name,
                    "table_name": dim_name,
                    "columns": dim_columns
                })
            
            # Prepare fact columns (measures)
            neo4j_fact_columns = []
            for measure in request.measures:
                col_name = measure.get("column", measure.get("name", "value"))
                neo4j_fact_columns.append({
                    "name": col_name,
                    "dtype": "NUMERIC(15,4)",
                    "description": measure.get("description", f"Measure: {col_name}")
                })
            
            # Register in Neo4j via robo-analyzer (for proper vectorization)
            # Convert to robo-analyzer format
            ra_dimensions = []
            for dim in neo4j_dimensions:
                ra_cols = [
                    DWColumnInfo(
                        name=col.get("name", ""),
                        dtype=col.get("dtype", "VARCHAR"),
                        description=col.get("description", ""),
                        is_pk=col.get("name", "").lower() == "id"
                    )
                    for col in dim.get("columns", [])
                ]
                ra_dimensions.append(DWDimensionInfo(
                    name=dim.get("table_name", dim.get("name", "")),
                    columns=ra_cols,
                    source_tables=request.source_tables
                ))
            
            # Build fact table FK columns
            ra_fact_cols = []
            for col in neo4j_fact_columns:
                col_name = col.get("name", "")
                is_fk = col_name.endswith("_id") and col_name.startswith("dim_")
                fk_target = None
                if is_fk:
                    dim_name = col_name.replace("_id", "")
                    fk_target = f"{request.dw_schema}.{dim_name}"
                
                ra_fact_cols.append(DWColumnInfo(
                    name=col_name,
                    dtype=col.get("dtype", "VARCHAR"),
                    description=col.get("description", ""),
                    is_fk=is_fk,
                    fk_target_table=fk_target
                ))
            
            ra_fact = DWFactTableInfo(
                name=fact_name,
                columns=ra_fact_cols,
                source_tables=request.source_tables
            )
            
            # Call robo-analyzer API
            neo4j_result = await robo_analyzer_client.register_star_schema(
                cube_name=request.cube_name,
                fact_table=ra_fact,
                dimensions=ra_dimensions,
                db_name="meetingroom",
                dw_schema=request.dw_schema,
                create_embeddings=True
            )
            
            # If robo-analyzer failed, fallback to direct Neo4j (without embeddings)
            if not neo4j_result.get("success") and neo4j_result.get("fallback_required"):
                async with neo4j_client:
                    neo4j_result = await neo4j_client.register_star_schema(
                        cube_name=request.cube_name,
                        fact_table_name=fact_name,
                        fact_columns=neo4j_fact_columns,
                        dimensions=neo4j_dimensions,
                        dw_schema=request.dw_schema,
                        db_name="meetingroom"
                    )
                    neo4j_result["fallback_used"] = True
            
            results["steps"].append({
                "step": "neo4j_register",
                "status": "success" if neo4j_result.get("success", True) else "partial",
                "details": neo4j_result
            })
        except Exception as e:
            results["errors"].append(f"Neo4j registration: {str(e)}")
        
    except Exception as e:
        results["success"] = False
        results["errors"].append(str(e))
    
    return results


# ============== Direct ETL Execution (No Airflow) ==============

class DirectETLRequest(BaseModel):
    """Request for direct ETL execution."""
    cube_name: str
    fact_table: str
    dimension_tables: List[str] = []
    source_tables: List[str] = []
    mappings: List[Dict[str, Any]] = []
    dw_schema: str = "dw"
    sync_mode: str = "full"
    incremental_column: Optional[str] = None


@router.post("/execute-direct")
async def execute_direct_etl(request: DirectETLRequest):
    """Execute ETL directly using Python (no Airflow).
    
    This endpoint executes the ETL pipeline immediately without 
    going through Airflow. Useful for quick testing or one-off loads.
    """
    from ..services.direct_etl_service import get_direct_etl_service
    
    direct_service = get_direct_etl_service()
    
    # Build config dict from request
    etl_config = {
        "cube_name": request.cube_name,
        "fact_table": request.fact_table,
        "dimension_tables": request.dimension_tables,
        "source_tables": request.source_tables,
        "mappings": request.mappings,
        "dw_schema": request.dw_schema,
        "sync_mode": request.sync_mode,
        "incremental_column": request.incremental_column
    }
    
    # Execute ETL
    result = direct_service.execute_etl(etl_config)
    
    return {
        "success": result.success,
        "cube_name": result.cube_name,
        "started_at": result.started_at,
        "completed_at": result.completed_at,
        "steps": result.steps,
        "total_rows_processed": result.total_rows_processed,
        "error": result.error
    }


@router.post("/execute-direct/{cube_name}")
async def execute_direct_etl_by_cube(cube_name: str):
    """Execute ETL for a specific cube directly using Python.
    
    Loads the ETL configuration for the cube and executes it immediately.
    """
    from ..services.direct_etl_service import get_direct_etl_service
    
    # Get ETL config for the cube
    config = etl_service.get_etl_config(cube_name)
    if not config:
        raise HTTPException(status_code=404, detail=f"ETL config not found for cube: {cube_name}")
    
    direct_service = get_direct_etl_service()
    
    # Build config dict
    etl_config = {
        "cube_name": cube_name,
        "fact_table": config.fact_table,
        "dimension_tables": config.dimension_tables,
        "source_tables": config.source_tables,
        "mappings": [m.__dict__ if hasattr(m, '__dict__') else m for m in config.mappings],
        "dw_schema": config.dw_schema,
        "sync_mode": config.sync_mode,
        "incremental_column": config.incremental_column
    }
    
    # Execute ETL
    result = direct_service.execute_etl(etl_config)
    
    return {
        "success": result.success,
        "cube_name": result.cube_name,
        "started_at": result.started_at,
        "completed_at": result.completed_at,
        "steps": result.steps,
        "total_rows_processed": result.total_rows_processed,
        "error": result.error
    }


@router.get("/generate-script/{cube_name}")
async def generate_etl_script(cube_name: str):
    """Generate standalone Python ETL script for a cube.
    
    Returns Python code that can be saved and executed independently.
    """
    from ..services.direct_etl_service import get_direct_etl_service
    
    # Get ETL config for the cube
    config = etl_service.get_etl_config(cube_name)
    if not config:
        raise HTTPException(status_code=404, detail=f"ETL config not found for cube: {cube_name}")
    
    direct_service = get_direct_etl_service()
    
    # Build config dict
    etl_config = {
        "cube_name": cube_name,
        "fact_table": config.fact_table,
        "dimension_tables": config.dimension_tables,
        "source_tables": config.source_tables,
        "mappings": [m.__dict__ if hasattr(m, '__dict__') else m for m in config.mappings],
        "dw_schema": config.dw_schema,
        "sync_mode": config.sync_mode,
        "incremental_column": config.incremental_column
    }
    
    # Generate Python code
    python_code = direct_service.generate_python_code(etl_config)
    
    return {
        "cube_name": cube_name,
        "filename": f"etl_{cube_name}.py",
        "code": python_code
    }


# ============== Intelligent ETL Agent ==============

class ETLAgentRequest(BaseModel):
    """Request for intelligent ETL generation."""
    cube_name: str
    cube_description: str
    dimensions: List[str]
    measures: List[str]


@router.post("/agent/generate")
async def generate_etl_with_agent(request: ETLAgentRequest):
    """Generate ETL using LangGraph-based intelligent agent.
    
    This endpoint uses an AI agent that:
    1. Analyzes source tables from the database
    2. Designs dimension ETL strategies
    3. Designs fact table ETL strategy
    4. Tests each SQL query
    5. Generates validated Python ETL script
    
    Returns detailed reasoning log and validated ETL configuration.
    """
    from ..services.etl_agent import get_etl_agent
    
    agent = get_etl_agent()
    
    result = await agent.generate_etl(
        cube_name=request.cube_name,
        cube_description=request.cube_description,
        dimensions=request.dimensions,
        measures=request.measures
    )
    
    return result


@router.post("/agent/generate-script/{cube_name}")
async def generate_script_with_agent(cube_name: str):
    """Generate ETL script using intelligent agent for existing cube.
    
    Loads cube metadata and uses AI agent to generate validated ETL.
    """
    from ..services.etl_agent import get_etl_agent
    
    # Get existing cube metadata
    config = etl_service.get_etl_config(cube_name)
    if not config:
        raise HTTPException(status_code=404, detail=f"ETL config not found for cube: {cube_name}")
    
    agent = get_etl_agent()
    
    # Extract dimensions and measures from config
    dimensions = [d.split(".")[-1] if "." in d else d for d in config.dimension_tables]
    measures = [m.get("name", m.get("column", "")) if isinstance(m, dict) else str(m) 
                for m in config.mappings if isinstance(m, dict) and m.get("target_table", "").startswith("fact")]
    
    if not measures:
        measures = ["value"]  # Default
    
    # Convert config to dict for the agent
    etl_config_dict = {
        "cube_name": config.cube_name,
        "fact_table": config.fact_table,
        "dimension_tables": config.dimension_tables,
        "source_tables": config.source_tables,
        "mappings": [m.model_dump() if hasattr(m, 'model_dump') else m for m in config.mappings],
        "dw_schema": config.dw_schema,
        "sync_mode": config.sync_mode
    }
    
    result = await agent.generate_etl(
        cube_name=cube_name,
        cube_description=f"ETL for {cube_name} cube",
        dimensions=dimensions,
        measures=measures,
        etl_config=etl_config_dict  # Pass the ETL config to the agent
    )
    
    return result


@router.get("/agent/generate-script-stream/{cube_name}")
async def generate_script_with_agent_stream(cube_name: str):
    """Generate ETL script using intelligent agent with SSE streaming.
    
    Returns Server-Sent Events (SSE) with real-time progress updates.
    Event types:
    - start: Agent started
    - log: Reasoning log message
    - code: Generated code (partial or complete)
    - sql: Generated SQL statement
    - status: Status update
    - dimensions: Dimension strategies
    - test: Test result
    - complete: Agent completed
    - error: Error occurred
    """
    from fastapi.responses import StreamingResponse
    from ..services.etl_agent import get_etl_agent
    import json
    
    # Get existing cube metadata
    config = etl_service.get_etl_config(cube_name)
    if not config:
        raise HTTPException(status_code=404, detail=f"ETL config not found for cube: {cube_name}")
    
    agent = get_etl_agent()
    
    # Extract dimensions and measures from config
    dimensions = [d.split(".")[-1] if "." in d else d for d in config.dimension_tables]
    
    # Convert mappings to dicts if they're dataclass objects
    from dataclasses import asdict, is_dataclass
    mappings_list = []
    for m in config.mappings:
        if is_dataclass(m) and not isinstance(m, type):
            mappings_list.append(asdict(m))
        elif isinstance(m, dict):
            mappings_list.append(m)
        else:
            # Try to convert to dict
            mappings_list.append({"source_table": str(m), "target_table": "", "source_column": "", "target_column": ""})
    
    measures = [m.get("target_column", m.get("column", "")) 
                for m in mappings_list 
                if m.get("target_table", "").startswith("fact")]
    
    if not measures:
        measures = ["value"]  # Default
    
    # Convert config to dict for agent (includes source_tables, mappings, etc.)
    etl_config_dict = {
        "cube_name": config.cube_name,
        "fact_table": config.fact_table,
        "dimension_tables": config.dimension_tables,
        "source_tables": config.source_tables,
        "mappings": mappings_list,  # Use converted list
        "dw_schema": config.dw_schema,
        "sync_mode": config.sync_mode
    }
    
    # Get cube description from store if available
    cube_description = f"ETL for {cube_name} cube - analyzing data from {', '.join(config.source_tables)}"
    
    async def event_generator():
        """Generate SSE events."""
        try:
            async for event in agent.generate_etl_streaming(
                cube_name=cube_name,
                cube_description=cube_description,
                dimensions=dimensions,
                measures=measures,
                etl_config=etl_config_dict  # Pass full ETL config
            ):
                event_type = event.get("event", "message")
                data = json.dumps(event.get("data", {}), ensure_ascii=False)
                yield f"event: {event_type}\ndata: {data}\n\n"
        except Exception as e:
            error_data = json.dumps({"error": str(e)}, ensure_ascii=False)
            yield f"event: error\ndata: {error_data}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "X-Accel-Buffering": "no"
        }
    )


# ============== Script Save/Load/Execute ==============

# In-memory storage for saved scripts (could be moved to file/database)
import os
import json
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent.parent / "data" / "scripts"
SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)


class SaveScriptRequest(BaseModel):
    """Request to save an ETL script."""
    code: str
    filename: Optional[str] = None


@router.post("/script/save/{cube_name}")
async def save_etl_script(cube_name: str, request: SaveScriptRequest):
    """Save an ETL script for a cube.
    
    The script will be persisted and can be loaded later.
    """
    script_path = SCRIPTS_DIR / f"{cube_name}.py"
    meta_path = SCRIPTS_DIR / f"{cube_name}.meta.json"
    
    try:
        # Save the script
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(request.code)
        
        # Save metadata
        metadata = {
            "cube_name": cube_name,
            "filename": request.filename or f"etl_{cube_name}.py",
            "saved_at": datetime.now().isoformat(),
            "code_length": len(request.code)
        }
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return {
            "success": True,
            "message": f"Script saved for {cube_name}",
            "path": str(script_path),
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save script: {str(e)}")


@router.get("/script/load/{cube_name}")
async def load_etl_script(cube_name: str):
    """Load a saved ETL script for a cube.
    
    Returns the script code and metadata if it exists.
    """
    script_path = SCRIPTS_DIR / f"{cube_name}.py"
    meta_path = SCRIPTS_DIR / f"{cube_name}.meta.json"
    
    if not script_path.exists():
        return {
            "exists": False,
            "cube_name": cube_name,
            "message": "No saved script found"
        }
    
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            code = f.read()
        
        metadata = {}
        if meta_path.exists():
            with open(meta_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        
        return {
            "exists": True,
            "cube_name": cube_name,
            "code": code,
            "filename": metadata.get("filename", f"etl_{cube_name}.py"),
            "saved_at": metadata.get("saved_at"),
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load script: {str(e)}")


class ExecuteScriptRequest(BaseModel):
    """Request body for executing ETL script."""
    sync_mode: str = "full"  # 'full' or 'incremental'
    last_sync: Optional[str] = None  # For incremental mode, the last sync timestamp


@router.post("/script/execute/{cube_name}")
async def execute_saved_script(cube_name: str, request: ExecuteScriptRequest = None):
    """Execute a saved ETL script for a cube.
    
    Runs the saved Python script directly with the specified sync mode.
    
    Args:
        cube_name: Name of the cube
        request: Optional request body with sync_mode ('full' or 'incremental')
    """
    from datetime import datetime
    import subprocess
    import tempfile
    
    # Handle case where request body is not provided
    sync_mode = "full"
    last_sync = None
    if request:
        sync_mode = request.sync_mode or "full"
        last_sync = request.last_sync
    
    script_path = SCRIPTS_DIR / f"{cube_name}.py"
    
    if not script_path.exists():
        raise HTTPException(status_code=404, detail=f"No saved script found for {cube_name}")
    
    started_at = datetime.now().isoformat()
    steps = []
    
    try:
        # Read the script
        with open(script_path, 'r', encoding='utf-8') as f:
            script_code = f.read()
        
        # Get database connection info from settings
        from ..core.config import get_settings
        settings = get_settings()
        db_host = settings.oltp_db_host
        db_port = str(settings.oltp_db_port)
        db_user = settings.oltp_db_user
        db_password = settings.oltp_db_password
        db_name = settings.oltp_db_name
        
        # Execute the script with environment variables (ETL_DB_* prefix for security)
        # Include sync mode and last sync timestamp
        env = os.environ.copy()
        env.update({
            'ETL_DB_HOST': db_host,
            'ETL_DB_PORT': db_port,
            'ETL_DB_USER': db_user,
            'ETL_DB_PASSWORD': db_password,
            'ETL_DB_NAME': db_name,
            'ETL_SYNC_MODE': sync_mode,  # 'full' or 'incremental'
            'ETL_LAST_SYNC': last_sync or '1900-01-01'  # For incremental mode
        })
        
        # Create a temporary file with the script
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as tf:
            tf.write(script_code)
            temp_script_path = tf.name
        
        try:
            # Run the script
            result = subprocess.run(
                ['python3', temp_script_path],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                env=env
            )
            
            if result.returncode == 0:
                steps.append({
                    "step": "execute_script",
                    "status": "success",
                    "message": "Script executed successfully",
                    "output": result.stdout[:2000] if result.stdout else None
                })
                
                # Register DW tables in Neo4j after successful ETL
                neo4j_registered = False
                neo4j_error = None
                try:
                    # Get ETL config to know what tables were created
                    config = etl_service.get_etl_config(cube_name)
                    if config:
                        # Build dimension info from config
                        dimensions = []
                        for dim_table in config.dimension_tables:
                            dim_name = dim_table.split(".")[-1] if "." in dim_table else dim_table
                            # Extract columns from mappings that target this dimension
                            dim_columns = [
                                {"name": m.target_column, "dtype": "VARCHAR", "description": f"From {m.source_table}.{m.source_column}"}
                                for m in config.mappings
                                if m.target_table == dim_table
                            ]
                            if not dim_columns:
                                dim_columns = [{"name": "value", "dtype": "VARCHAR", "description": "Dimension value"}]
                            dimensions.append({
                                "name": dim_name,
                                "table_name": dim_name,
                                "columns": dim_columns
                            })
                        
                        # Build fact columns (measures)
                        fact_table_name = config.fact_table.split(".")[-1] if "." in config.fact_table else config.fact_table
                        fact_columns = [
                            {"name": m.target_column, "dtype": "NUMERIC", "description": f"Measure from {m.source_table}"}
                            for m in config.mappings
                            if m.target_table.startswith("fact")
                        ]
                        if not fact_columns:
                            fact_columns = [{"name": "value", "dtype": "NUMERIC", "description": "Measure value"}]
                        
                        # Register in Neo4j via robo-analyzer (for proper vectorization)
                        dw_schema = config.dw_schema or "dw"
                        
                        # Build robo-analyzer format dimensions
                        ra_dimensions = []
                        for dim in dimensions:
                            ra_cols = [
                                DWColumnInfo(
                                    name=col.get("name", ""),
                                    dtype=col.get("dtype", "VARCHAR"),
                                    description=col.get("description", ""),
                                    is_pk=col.get("name", "").lower() == "id"
                                )
                                for col in dim.get("columns", [])
                            ]
                            # Add id column if not present
                            if not any(c.name == "id" for c in ra_cols):
                                ra_cols.insert(0, DWColumnInfo(name="id", dtype="SERIAL", description="Primary key", is_pk=True))
                            
                            ra_dimensions.append(DWDimensionInfo(
                                name=dim.get("table_name", dim.get("name", "")),
                                columns=ra_cols,
                                source_tables=config.source_tables
                            ))
                        
                        # Build fact table with FK columns
                        ra_fact_cols = []
                        # Add FK columns for each dimension
                        for dim in dimensions:
                            dim_name = dim.get("table_name", dim.get("name", ""))
                            fk_col_name = f"{dim_name}_id"
                            ra_fact_cols.append(DWColumnInfo(
                                name=fk_col_name,
                                dtype="INTEGER",
                                description=f"FK to {dim_name}",
                                is_fk=True,
                                fk_target_table=f"{dw_schema}.{dim_name}"
                            ))
                        # Add measure columns
                        for col in fact_columns:
                            ra_fact_cols.append(DWColumnInfo(
                                name=col.get("name", ""),
                                dtype=col.get("dtype", "NUMERIC"),
                                description=col.get("description", "")
                            ))
                        
                        ra_fact = DWFactTableInfo(
                            name=fact_table_name,
                            columns=ra_fact_cols,
                            source_tables=config.source_tables
                        )
                        
                        # Call robo-analyzer API
                        neo4j_result = await robo_analyzer_client.register_star_schema(
                            cube_name=cube_name,
                            fact_table=ra_fact,
                            dimensions=ra_dimensions,
                            db_name=db_name,
                            dw_schema=dw_schema,
                            create_embeddings=True
                        )
                        
                        # If robo-analyzer failed, fallback to direct Neo4j (without embeddings)
                        if not neo4j_result.get("success") and neo4j_result.get("fallback_required"):
                            from ..services.neo4j_client import neo4j_client as fallback_neo4j
                            async with fallback_neo4j:
                                neo4j_result = await fallback_neo4j.register_star_schema(
                                    cube_name=cube_name,
                                    fact_table_name=fact_table_name,
                                    fact_columns=fact_columns,
                                    dimensions=dimensions,
                                    dw_schema=dw_schema,
                                    db_name=db_name,
                                    source_tables=config.source_tables
                                )
                                neo4j_result["fallback_used"] = True
                        
                        neo4j_registered = neo4j_result.get("success", False)
                        steps.append({
                            "step": "register_neo4j_metadata",
                            "status": "success",
                            "message": f"DW 테이블 메타데이터 등록 완료: {neo4j_result.get('tables_created', [])} (FK: {neo4j_result.get('fk_relationships', 0)}, Lineage: {neo4j_result.get('lineage_relationships', 0)})",
                            "tables_created": neo4j_result.get("tables_created", []),
                            "fk_relationships": neo4j_result.get("fk_relationships", 0),
                            "lineage_relationships": neo4j_result.get("lineage_relationships", 0)
                        })
                except Exception as neo4j_ex:
                    neo4j_error = str(neo4j_ex)
                    steps.append({
                        "step": "register_neo4j_metadata",
                        "status": "warning",
                        "message": f"Neo4j 메타데이터 등록 실패 (ETL은 성공): {neo4j_error}"
                    })
                
                return {
                    "success": True,
                    "cube_name": cube_name,
                    "started_at": started_at,
                    "completed_at": datetime.now().isoformat(),
                    "steps": steps,
                    "output": result.stdout,
                    "neo4j_registered": neo4j_registered,
                    "total_rows_processed": 0  # Would need to parse from output
                }
            else:
                steps.append({
                    "step": "execute_script",
                    "status": "error",
                    "message": result.stderr[:1000] if result.stderr else "Script failed"
                })
                
                return {
                    "success": False,
                    "cube_name": cube_name,
                    "started_at": started_at,
                    "completed_at": datetime.now().isoformat(),
                    "steps": steps,
                    "error": result.stderr,
                    "total_rows_processed": 0
                }
        finally:
            # Clean up temp file
            os.unlink(temp_script_path)
            
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "cube_name": cube_name,
            "started_at": started_at,
            "completed_at": datetime.now().isoformat(),
            "steps": steps,
            "error": "Script execution timed out (5 minutes)",
            "total_rows_processed": 0
        }
    except Exception as e:
        return {
            "success": False,
            "cube_name": cube_name,
            "started_at": started_at,
            "completed_at": datetime.now().isoformat(),
            "steps": steps,
            "error": str(e),
            "total_rows_processed": 0
        }


class ExecuteWithRetryRequest(BaseModel):
    """Request body for executing ETL script with auto-retry."""
    sync_mode: str = "full"
    max_retries: int = 3  # Maximum number of retry attempts


@router.get("/script/execute-with-retry/{cube_name}")
async def execute_script_with_retry(
    cube_name: str, 
    sync_mode: str = "full",
    max_retries: int = 3
):
    """Execute ETL script with automatic error recovery and retry.
    
    This endpoint streams execution progress via SSE and automatically
    regenerates the script using the ETL agent if execution fails.
    
    Args:
        cube_name: Name of the cube
        sync_mode: 'full' or 'incremental'
        max_retries: Maximum number of retry attempts (default: 3)
    """
    from fastapi.responses import StreamingResponse
    from datetime import datetime
    import subprocess
    import tempfile
    import asyncio
    
    async def execute_with_feedback_loop():
        """Generator that streams execution progress and handles retries."""
        attempt = 0
        last_error = None
        
        while attempt < max_retries:
            attempt += 1
            
            # Send progress update
            yield f"data: {json.dumps({'type': 'progress', 'attempt': attempt, 'max_retries': max_retries, 'message': f'🚀 시도 {attempt}/{max_retries}: ETL 스크립트 실행 중...'})}\n\n"
            
            script_path = SCRIPTS_DIR / f"{cube_name}.py"
            
            if not script_path.exists():
                yield f"data: {json.dumps({'type': 'error', 'message': f'스크립트 파일이 없습니다: {cube_name}.py', 'final': True})}\n\n"
                return
            
            try:
                # Read the script
                with open(script_path, 'r', encoding='utf-8') as f:
                    script_code = f.read()
                
                # Get database connection info
                from ..core.config import get_settings
                settings = get_settings()
                
                env = os.environ.copy()
                env.update({
                    'ETL_DB_HOST': settings.oltp_db_host,
                    'ETL_DB_PORT': str(settings.oltp_db_port),
                    'ETL_DB_USER': settings.oltp_db_user,
                    'ETL_DB_PASSWORD': settings.oltp_db_password,
                    'ETL_DB_NAME': settings.oltp_db_name,
                    'ETL_SYNC_MODE': sync_mode,
                    'ETL_LAST_SYNC': '1900-01-01'
                })
                
                # Create temp script file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as tf:
                    tf.write(script_code)
                    temp_script_path = tf.name
                
                try:
                    # Run the script
                    result = subprocess.run(
                        ['python3', temp_script_path],
                        capture_output=True,
                        text=True,
                        timeout=300,
                        env=env
                    )
                    
                    if result.returncode == 0:
                        # Success!
                        yield f"data: {json.dumps({'type': 'log', 'message': result.stdout[-2000:] if result.stdout else 'No output'})}\n\n"
                        yield f"data: {json.dumps({'type': 'success', 'attempt': attempt, 'message': f'✅ ETL 실행 완료! (시도 {attempt}회)', 'output': result.stdout[-1000:] if result.stdout else ''})}\n\n"
                        
                        # Register to Neo4j via robo-analyzer
                        yield f"data: {json.dumps({'type': 'progress', 'message': '📊 Neo4j 메타데이터 등록 중...'})}\n\n"
                        
                        # TODO: Add Neo4j registration here
                        
                        yield f"data: {json.dumps({'type': 'complete', 'success': True, 'message': '🎉 ETL 파이프라인 완료!'})}\n\n"
                        return
                    else:
                        # Execution failed
                        error_msg = result.stderr or result.stdout or "Unknown error"
                        last_error = error_msg
                        
                        yield f"data: {json.dumps({'type': 'error', 'attempt': attempt, 'message': f'❌ 실행 오류 (시도 {attempt}): {error_msg[:500]}'})}\n\n"
                        
                        # If we have retries left, regenerate the script
                        if attempt < max_retries:
                            yield f"data: {json.dumps({'type': 'regenerating', 'message': f'🔄 에이전트가 스크립트를 수정 중... (오류 컨텍스트 전달)'})}\n\n"
                            
                            # Call agent to regenerate with error context
                            try:
                                from ..services.etl_agent import ETLAgent
                                
                                agent = ETLAgent()
                                
                                # Get existing ETL config
                                config = etl_service.get_etl_config(cube_name)
                                etl_config_dict = None
                                if config:
                                    etl_config_dict = {
                                        "cube_name": config.cube_name,
                                        "fact_table": config.fact_table,
                                        "dimension_tables": config.dimension_tables,
                                        "source_tables": config.source_tables,
                                        "mappings": [m.__dict__ if hasattr(m, '__dict__') else m for m in config.mappings],
                                        "dw_schema": config.dw_schema
                                    }
                                
                                # Regenerate with error context
                                regeneration_context = {
                                    "errors": [{"type": "execution_error", "message": error_msg[:1000]}],
                                    "hints": [
                                        "PostgreSQL 대소문자 처리: 대문자 테이블명은 쌍따옴표 필요",
                                        "테이블 존재 여부 확인: 스키마명.테이블명 형식 사용",
                                        "FROM 절에 모든 참조 테이블 포함 확인"
                                    ]
                                }
                                
                                yield f"data: {json.dumps({'type': 'agent_reasoning', 'message': f'📝 오류 분석: {error_msg[:200]}...'})}\n\n"
                                
                                # Generate new script
                                async for event in agent.generate_etl_streaming(
                                    cube_name=cube_name,
                                    cube_description=f"Fix ETL for {cube_name}",
                                    target_dimensions=config.dimension_tables if config else [],
                                    target_measures=[],
                                    etl_config=etl_config_dict
                                ):
                                    if event.get("type") == "reasoning":
                                        yield f"data: {json.dumps({'type': 'agent_reasoning', 'message': event.get('message', '')[:200]})}\n\n"
                                    elif event.get("type") == "script":
                                        new_script = event.get("script", "")
                                        if new_script:
                                            # Save new script
                                            with open(script_path, 'w', encoding='utf-8') as f:
                                                f.write(new_script)
                                            yield f"data: {json.dumps({'type': 'script_updated', 'message': '✅ 스크립트가 수정되었습니다. 재시도합니다...'})}\n\n"
                                
                                await asyncio.sleep(1)  # Brief pause before retry
                                
                            except Exception as agent_error:
                                yield f"data: {json.dumps({'type': 'warning', 'message': f'⚠️ 에이전트 재생성 실패: {str(agent_error)[:200]}'})}\n\n"
                        
                finally:
                    # Cleanup temp file
                    if os.path.exists(temp_script_path):
                        os.unlink(temp_script_path)
                        
            except subprocess.TimeoutExpired:
                last_error = "Execution timeout (5 minutes)"
                yield f"data: {json.dumps({'type': 'error', 'attempt': attempt, 'message': '⏰ 실행 시간 초과 (5분)'})}\n\n"
            except Exception as e:
                last_error = str(e)
                yield f"data: {json.dumps({'type': 'error', 'attempt': attempt, 'message': f'❌ 예외 발생: {str(e)[:300]}'})}\n\n"
        
        # All retries exhausted
        yield f"data: {json.dumps({'type': 'complete', 'success': False, 'message': f'❌ 최대 재시도 횟수({max_retries}회) 초과. 마지막 오류: {last_error[:300] if last_error else "Unknown"}'})}\n\n"
    
    return StreamingResponse(
        execute_with_feedback_loop(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.delete("/script/{cube_name}")
async def delete_saved_script(cube_name: str):
    """Delete a saved ETL script for a cube."""
    script_path = SCRIPTS_DIR / f"{cube_name}.py"
    meta_path = SCRIPTS_DIR / f"{cube_name}.meta.json"
    
    deleted = False
    
    if script_path.exists():
        script_path.unlink()
        deleted = True
    
    if meta_path.exists():
        meta_path.unlink()
    
    return {
        "success": True,
        "deleted": deleted,
        "cube_name": cube_name
    }


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


@router.get("/neo4j/dw-schema")
async def get_dw_schema_from_neo4j():
    """Get DW schema structure from Neo4j for verification."""
    try:
        async with neo4j_client:
            # Query Schema node and connected tables
            result = await neo4j_client.execute_query("""
                MATCH (s:Schema {name: 'dw'})
                OPTIONAL MATCH (s)<-[:BELONGS_TO]-(t:Table)
                OPTIONAL MATCH (t)-[:FK_TO_TABLE]->(dim:Table)
                OPTIONAL MATCH (t)-[:DERIVED_FROM]->(src:Table)
                RETURN s.name as schema_name, 
                       s.type as schema_type,
                       collect(DISTINCT {
                           name: t.name, 
                           type: t.table_type, 
                           cube: t.cube_name
                       }) as tables,
                       collect(DISTINCT {
                           from: t.name,
                           to: dim.name,
                           rel: 'FK_TO_TABLE'
                       }) as fk_relationships,
                       collect(DISTINCT {
                           dw_table: t.name,
                           source_table: src.name,
                           source_schema: src.schema
                       }) as lineage
            """)
            
            return {
                "success": True,
                "data": result
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }



