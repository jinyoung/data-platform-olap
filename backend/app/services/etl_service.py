"""ETL Service for OLTP → OLAP data pipeline.

This service handles:
1. Source table exploration from Neo4j catalog
2. ETL configuration with user interaction
3. DW schema creation (star schema)
4. Data synchronization (diff-based ETL)
5. Lineage registration back to Neo4j
"""
import asyncio
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum

import asyncpg
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

from ..core.config import get_settings
from .neo4j_client import neo4j_client

# Storage directory for ETL configs
STORAGE_DIR = Path(__file__).parent.parent.parent / "data"
ETL_CONFIGS_FILE = STORAGE_DIR / "etl_configs.json"


class ETLStatus(str, Enum):
    """ETL job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ETLMapping:
    """Mapping between source and target columns."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transformation: str = ""  # SQL expression for transformation


@dataclass
class ETLConfig:
    """ETL configuration for a cube."""
    cube_name: str
    fact_table: str
    dimension_tables: List[str]
    source_tables: List[str]
    mappings: List[ETLMapping]
    dw_schema: str = "dw"
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_sync: Optional[str] = None
    sync_mode: str = "full"  # full, incremental
    incremental_column: Optional[str] = None  # Column for incremental sync
    
    def to_dict(self) -> Dict:
        return {
            "cube_name": self.cube_name,
            "fact_table": self.fact_table,
            "dimension_tables": self.dimension_tables,
            "source_tables": self.source_tables,
            "mappings": [asdict(m) for m in self.mappings],
            "dw_schema": self.dw_schema,
            "created_at": self.created_at,
            "last_sync": self.last_sync,
            "sync_mode": self.sync_mode,
            "incremental_column": self.incremental_column
        }


@dataclass
class SyncResult:
    """Result of ETL sync operation."""
    status: ETLStatus
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    duration_ms: float = 0
    error: Optional[str] = None
    details: Dict = field(default_factory=dict)


class ETLService:
    """ETL Service for managing OLTP → OLAP data pipelines."""
    
    def __init__(self):
        self.settings = get_settings()
        self._configs: Dict[str, ETLConfig] = {}  # In-memory storage
        self._pool: Optional[asyncpg.Pool] = None
        self._initialized = False
    
    def _ensure_initialized(self) -> None:
        """Load persisted ETL configs on first access."""
        if not self._initialized:
            self._load_configs_from_file()
            self._initialized = True
    
    def _load_configs_from_file(self) -> None:
        """Load ETL configs from JSON file."""
        if ETL_CONFIGS_FILE.exists():
            try:
                with open(ETL_CONFIGS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for name, config_dict in data.items():
                        # Convert mappings back to ETLMapping objects
                        mappings = [ETLMapping(**m) for m in config_dict.get('mappings', [])]
                        # Create config with mappings included
                        self._configs[name] = ETLConfig(
                            cube_name=config_dict['cube_name'],
                            fact_table=config_dict['fact_table'],
                            dimension_tables=config_dict['dimension_tables'],
                            source_tables=config_dict['source_tables'],
                            mappings=mappings,
                            dw_schema=config_dict.get('dw_schema', 'dw'),
                            created_at=config_dict.get('created_at', ''),
                            last_sync=config_dict.get('last_sync'),
                            sync_mode=config_dict.get('sync_mode', 'full'),
                            incremental_column=config_dict.get('incremental_column')
                        )
                print(f"Loaded {len(self._configs)} ETL configs from {ETL_CONFIGS_FILE}")
            except Exception as e:
                print(f"Failed to load ETL configs from file: {e}")
    
    def _save_configs_to_file(self) -> None:
        """Save ETL configs to JSON file."""
        try:
            STORAGE_DIR.mkdir(parents=True, exist_ok=True)
            data = {name: config.to_dict() for name, config in self._configs.items()}
            with open(ETL_CONFIGS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(self._configs)} ETL configs to {ETL_CONFIGS_FILE}")
        except Exception as e:
            print(f"Failed to save ETL configs to file: {e}")
    
    async def get_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self.settings.database_url,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
        return self._pool
    
    # =========================================================================
    # Source Catalog Exploration
    # =========================================================================
    
    async def explore_source_catalog(
        self,
        user_id: str = None,
        project_name: str = None,
        schema: str = None,
        search: str = None
    ) -> Dict[str, Any]:
        """Explore source tables from Neo4j catalog.
        
        Returns tables with their columns and relationships.
        """
        async with neo4j_client:
            tables = await neo4j_client.get_tables(
                user_id=user_id,
                project_name=project_name,
                schema=schema,
                search=search
            )
            
            relationships = await neo4j_client.get_table_relationships(
                user_id=user_id,
                project_name=project_name
            )
            
            schemas = await neo4j_client.get_schemas(
                user_id=user_id,
                project_name=project_name
            )
        
        return {
            "tables": tables,
            "relationships": relationships,
            "schemas": schemas,
            "total_tables": len(tables)
        }
    
    async def get_table_details(
        self,
        table_name: str,
        schema: str = None,
        user_id: str = None,
        project_name: str = None
    ) -> Dict[str, Any]:
        """Get detailed information about a specific table."""
        async with neo4j_client:
            columns = await neo4j_client.get_table_columns(
                table_name=table_name,
                schema=schema,
                user_id=user_id,
                project_name=project_name
            )
        
        return {
            "table_name": table_name,
            "schema": schema,
            "columns": columns,
            "column_count": len(columns)
        }
    
    # =========================================================================
    # ETL Configuration with AI Assistance
    # =========================================================================
    
    async def suggest_etl_strategy(
        self,
        cube_description: str,
        available_tables: List[Dict]
    ) -> Dict[str, Any]:
        """Use LLM to suggest ETL strategy based on cube description and available tables.
        
        This is the ReAct-style interaction to help users configure ETL.
        """
        llm = ChatOpenAI(
            model=self.settings.openai_model,
            api_key=self.settings.openai_api_key,
            temperature=0.3
        )
        
        # Format available tables for context
        tables_context = "\n".join([
            f"- {t['schema']}.{t['name']}: {t.get('description', 'No description')} "
            f"(columns: {', '.join([c['name'] for c in t.get('columns', []) if c.get('name')])})"
            for t in available_tables
        ])
        
        system_prompt = """You are a data warehouse architect helping design ETL pipelines.
Given the cube description and available source tables, suggest:
1. Which tables should be used as fact table sources
2. Which tables should be used as dimension sources
3. How to map source columns to the star schema
4. Suggested ETL strategy (full refresh vs incremental)

OUTPUT FORMAT (JSON):
{
    "fact_sources": ["table1", "table2"],
    "dimension_sources": {
        "dim_time": ["source_table"],
        "dim_product": ["product_table"]
    },
    "suggested_mappings": [
        {"source": "table.column", "target": "fact_table.column", "transformation": "SUM(...)"}
    ],
    "sync_strategy": "incremental",
    "incremental_column": "updated_at",
    "reasoning": "Explanation of the strategy..."
}"""
        
        response = await llm.ainvoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"""Cube Description:
{cube_description}

Available Source Tables:
{tables_context}

Suggest the best ETL strategy for this OLAP cube.""")
        ])
        
        # Parse JSON response
        try:
            content = response.content.strip()
            if content.startswith("```"):
                lines = content.split("\n")
                content = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
            return json.loads(content)
        except json.JSONDecodeError:
            return {
                "error": "Failed to parse LLM response",
                "raw_response": response.content
            }
    
    async def create_etl_config(
        self,
        cube_name: str,
        fact_table: str,
        dimension_tables: List[str],
        source_tables: List[str],
        mappings: List[Dict],
        dw_schema: str = "dw",
        sync_mode: str = "full",
        incremental_column: str = None
    ) -> ETLConfig:
        """Create and store ETL configuration."""
        self._ensure_initialized()
        
        config = ETLConfig(
            cube_name=cube_name,
            fact_table=fact_table,
            dimension_tables=dimension_tables,
            source_tables=source_tables,
            mappings=[ETLMapping(**m) for m in mappings],
            dw_schema=dw_schema,
            sync_mode=sync_mode,
            incremental_column=incremental_column
        )
        
        self._configs[cube_name] = config
        self._save_configs_to_file()  # Persist to file
        return config
    
    def get_etl_config(self, cube_name: str) -> Optional[ETLConfig]:
        """Get ETL configuration for a cube."""
        self._ensure_initialized()
        return self._configs.get(cube_name)
    
    def delete_etl_config(self, cube_name: str) -> bool:
        """Delete ETL configuration for a cube."""
        self._ensure_initialized()
        if cube_name in self._configs:
            del self._configs[cube_name]
            self._save_configs_to_file()
            return True
        return False
    
    def get_all_etl_configs(self) -> Dict[str, ETLConfig]:
        """Get all ETL configurations."""
        self._ensure_initialized()
        return self._configs.copy()
    
    def clear_all_etl_configs(self) -> None:
        """Clear all ETL configurations."""
        self._ensure_initialized()
        self._configs.clear()
        self._save_configs_to_file()
    
    def _full_table_name(self, table: str, dw_schema: str) -> str:
        """Get full table name, avoiding double schema prefix."""
        if '.' in table:
            return table  # Already has schema
        return f"{dw_schema}.{table}"
    
    def _strip_schema(self, table: str) -> str:
        """Strip schema prefix from table name."""
        if '.' in table:
            return table.split('.')[-1]
        return table
    
    # =========================================================================
    # DW Schema Creation
    # =========================================================================
    
    async def create_dw_schema(self, schema_name: str = "dw") -> Dict:
        """Create the DW schema in PostgreSQL if it doesn't exist."""
        pool = await self.get_pool()
        
        async with pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        return {"success": True, "schema": schema_name}
    
    async def generate_star_schema_ddl(
        self,
        cube_name: str,
        fact_table_name: str,
        fact_columns: List[Dict],
        dimensions: List[Dict],
        dw_schema: str = "dw"
    ) -> str:
        """Generate DDL for star schema tables.
        
        Args:
            cube_name: Name of the cube
            fact_table_name: Name of the fact table
            fact_columns: List of {name, dtype, description} for fact columns
            dimensions: List of {name, table_name, columns: [{name, dtype}]}
            dw_schema: Schema name for DW tables
        
        Returns:
            SQL DDL statements
        """
        ddl_statements = []
        
        # Create schema
        ddl_statements.append(f"CREATE SCHEMA IF NOT EXISTS {dw_schema};")
        
        # Create dimension tables
        for dim in dimensions:
            dim_table = f"{dw_schema}.{dim['table_name']}"
            columns = ["id SERIAL PRIMARY KEY"]
            for col in dim.get("columns", []):
                col_def = f"{col['name']} {col.get('dtype', 'VARCHAR(255)')}"
                columns.append(col_def)
            columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
            columns.append("_etl_source_hash VARCHAR(64)")
            
            ddl_statements.append(f"""
CREATE TABLE IF NOT EXISTS {dim_table} (
    {', '.join(columns)}
);""")
        
        # Create fact table with FK references
        fact_table = f"{dw_schema}.{fact_table_name}"
        fact_cols = ["id SERIAL PRIMARY KEY"]
        
        # Add dimension foreign keys
        for dim in dimensions:
            fk_col = f"{dim['table_name']}_id"
            fact_cols.append(f"{fk_col} INTEGER REFERENCES {dw_schema}.{dim['table_name']}(id)")
        
        # Add measure columns
        for col in fact_columns:
            col_def = f"{col['name']} {col.get('dtype', 'NUMERIC')}"
            fact_cols.append(col_def)
        
        fact_cols.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
        fact_cols.append("_etl_source_hash VARCHAR(64)")
        
        ddl_statements.append(f"""
CREATE TABLE IF NOT EXISTS {fact_table} (
    {', '.join(fact_cols)}
);""")
        
        # Create indexes
        for dim in dimensions:
            fk_col = f"{dim['table_name']}_id"
            ddl_statements.append(
                f"CREATE INDEX IF NOT EXISTS idx_{fact_table_name}_{fk_col} "
                f"ON {fact_table}({fk_col});"
            )
        
        return "\n".join(ddl_statements)
    
    async def execute_ddl(self, ddl: str) -> Dict:
        """Execute DDL statements."""
        pool = await self.get_pool()
        
        results = []
        statements = [s.strip() for s in ddl.split(";") if s.strip()]
        
        async with pool.acquire() as conn:
            for stmt in statements:
                try:
                    await conn.execute(stmt)
                    results.append({"statement": stmt[:100], "status": "success"})
                except Exception as e:
                    results.append({"statement": stmt[:100], "status": "error", "error": str(e)})
        
        success_count = len([r for r in results if r["status"] == "success"])
        return {
            "success": success_count == len(statements),
            "executed": success_count,
            "failed": len(statements) - success_count,
            "details": results
        }
    
    # =========================================================================
    # ETL Sync Execution
    # =========================================================================
    
    async def sync_data(
        self,
        cube_name: str,
        force_full: bool = False
    ) -> SyncResult:
        """Execute ETL sync for a cube.
        
        This performs:
        1. Read source data from OLTP tables
        2. Calculate diff (for incremental sync)
        3. Transform and load into OLAP tables
        4. Update sync metadata
        """
        import time
        start_time = time.time()
        
        config = self.get_etl_config(cube_name)
        if not config:
            return SyncResult(
                status=ETLStatus.FAILED,
                error=f"No ETL config found for cube: {cube_name}"
            )
        
        try:
            pool = await self.get_pool()
            rows_inserted = 0
            rows_updated = 0
            details = {}
            
            async with pool.acquire() as conn:
                # Determine sync mode
                is_full_sync = force_full or config.sync_mode == "full" or config.last_sync is None
                
                if is_full_sync:
                    # Full refresh: truncate and reload
                    for dim_table in config.dimension_tables:
                        full_dim = self._full_table_name(dim_table, config.dw_schema)
                        await conn.execute(f"TRUNCATE TABLE {full_dim} CASCADE")
                    full_fact = self._full_table_name(config.fact_table, config.dw_schema)
                    await conn.execute(f"TRUNCATE TABLE {full_fact}")
                
                # Sync dimension tables
                for dim_table in config.dimension_tables:
                    dim_result = await self._sync_dimension(
                        conn, config, dim_table, is_full_sync
                    )
                    rows_inserted += dim_result.get("inserted", 0)
                    rows_updated += dim_result.get("updated", 0)
                    details[dim_table] = dim_result
                
                # Sync fact table
                fact_result = await self._sync_fact(
                    conn, config, is_full_sync
                )
                rows_inserted += fact_result.get("inserted", 0)
                rows_updated += fact_result.get("updated", 0)
                details[config.fact_table] = fact_result
            
            # Update last sync time
            config.last_sync = datetime.now().isoformat()
            
            duration_ms = (time.time() - start_time) * 1000
            
            return SyncResult(
                status=ETLStatus.COMPLETED,
                rows_inserted=rows_inserted,
                rows_updated=rows_updated,
                duration_ms=round(duration_ms, 2),
                details=details
            )
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return SyncResult(
                status=ETLStatus.FAILED,
                error=str(e),
                duration_ms=round(duration_ms, 2)
            )
    
    async def _sync_dimension(
        self,
        conn: asyncpg.Connection,
        config: ETLConfig,
        dim_table: str,
        is_full_sync: bool
    ) -> Dict:
        """Sync a dimension table."""
        # Find mappings for this dimension (match with or without schema prefix)
        dim_table_name = self._strip_schema(dim_table)
        dim_mappings = [m for m in config.mappings 
                       if m.target_table == dim_table 
                       or m.target_table == dim_table_name]
        
        if not dim_mappings:
            return {"status": "skipped", "reason": f"No mappings found for {dim_table}"}
        
        # Build INSERT query from source
        source_columns = []
        target_columns = []
        
        for mapping in dim_mappings:
            source_expr = mapping.transformation if mapping.transformation else f"{mapping.source_table}.{mapping.source_column}"
            source_columns.append(f"{source_expr} AS {mapping.target_column}")
            target_columns.append(mapping.target_column)
        
        # Get unique source tables
        source_tables = list(set([m.source_table for m in dim_mappings]))
        from_clause = ", ".join(source_tables)
        
        # Generate hash for deduplication
        coalesce_parts = [f"COALESCE({c}::text, '')" for c in target_columns]
        hash_expr = f"MD5(CONCAT({', '.join(coalesce_parts)}))"
        
        full_dim_table = self._full_table_name(dim_table, config.dw_schema)
        insert_sql = f"""
            INSERT INTO {full_dim_table} ({', '.join(target_columns)}, _etl_source_hash)
            SELECT DISTINCT {', '.join(source_columns)}, {hash_expr}
            FROM {from_clause}
            ON CONFLICT DO NOTHING
        """
        
        result = await conn.execute(insert_sql)
        inserted = int(result.split()[-1]) if result else 0
        
        return {"inserted": inserted, "updated": 0}
    
    async def _sync_fact(
        self,
        conn: asyncpg.Connection,
        config: ETLConfig,
        is_full_sync: bool
    ) -> Dict:
        """Sync the fact table."""
        # Find mappings for fact table (match with or without schema prefix)
        fact_table_name = self._strip_schema(config.fact_table)
        fact_mappings = [m for m in config.mappings 
                        if m.target_table == config.fact_table 
                        or m.target_table == fact_table_name]
        
        if not fact_mappings:
            return {"status": "skipped", "reason": "No mappings found"}
        
        # Build INSERT query
        source_columns = []
        target_columns = []
        
        for mapping in fact_mappings:
            source_expr = mapping.transformation if mapping.transformation else f"{mapping.source_table}.{mapping.source_column}"
            source_columns.append(f"{source_expr} AS {mapping.target_column}")
            target_columns.append(mapping.target_column)
        
        # Get unique source tables
        source_tables = list(set([m.source_table for m in fact_mappings]))
        from_clause = ", ".join(source_tables)
        
        full_fact_table = self._full_table_name(config.fact_table, config.dw_schema)
        insert_sql = f"""
            INSERT INTO {full_fact_table} ({', '.join(target_columns)})
            SELECT {', '.join(source_columns)}
            FROM {from_clause}
        """
        
        result = await conn.execute(insert_sql)
        inserted = int(result.split()[-1]) if result else 0
        
        return {"inserted": inserted, "updated": 0}
    
    # =========================================================================
    # Lineage Registration
    # =========================================================================
    
    async def register_lineage(
        self,
        cube_name: str,
        user_id: str,
        project_name: str
    ) -> Dict:
        """Register OLAP tables and lineage in Neo4j."""
        config = self.get_etl_config(cube_name)
        if not config:
            return {"error": f"No ETL config found for cube: {cube_name}"}
        
        results = []
        
        async with neo4j_client:
            # Register fact table
            fact_columns = [
                {"name": m.target_column, "dtype": "NUMERIC", "description": f"From {m.source_table}.{m.source_column}"}
                for m in config.mappings if m.target_table == config.fact_table
            ]
            
            fact_result = await neo4j_client.register_olap_table(
                table_name=config.fact_table,
                schema=config.dw_schema,
                columns=fact_columns,
                source_tables=config.source_tables,
                user_id=user_id,
                project_name=project_name,
                cube_name=cube_name
            )
            results.append(fact_result)
            
            # Register dimension tables
            for dim_table in config.dimension_tables:
                dim_columns = [
                    {"name": m.target_column, "dtype": "VARCHAR", "description": f"From {m.source_table}.{m.source_column}"}
                    for m in config.mappings if m.target_table == dim_table
                ]
                
                # Find source tables for this dimension
                dim_sources = list(set([
                    m.source_table for m in config.mappings 
                    if m.target_table == dim_table
                ]))
                
                dim_result = await neo4j_client.register_olap_table(
                    table_name=dim_table,
                    schema=config.dw_schema,
                    columns=dim_columns,
                    source_tables=dim_sources,
                    user_id=user_id,
                    project_name=project_name,
                    cube_name=cube_name
                )
                results.append(dim_result)
        
        return {
            "success": True,
            "tables_registered": len(results),
            "details": results
        }


# Global service instance
etl_service = ETLService()

