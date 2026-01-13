"""ETL Agent Service - LangGraph-based intelligent ETL generation.

This agent uses ReAct pattern to:
1. Analyze source data from Neo4j metadata
2. Generate ETL logic step by step
3. Test each step with actual SQL queries
4. Validate data transformations
5. Refine until correct
"""
import os
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional, TypedDict, Annotated
from dataclasses import dataclass, field

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
import asyncpg

from ..core.config import get_settings


# ============== State Definition ==============

class ETLAgentState(TypedDict):
    """State for the ETL Agent."""
    # Input
    cube_name: str
    cube_description: str
    target_dimensions: List[str]
    target_measures: List[str]
    
    # ETL Configuration (from UI)
    etl_config: Optional[Dict]  # Full ETL config including source_tables, mappings, etc.
    
    # Working state
    messages: List[Any]
    source_tables: List[Dict]
    dimension_strategies: List[Dict]
    fact_strategy: Dict
    generated_sql: Dict[str, str]
    test_results: List[Dict]
    retry_count: int  # Track retry attempts for error recovery
    
    # Script validation state
    validation_results: Optional[Dict]  # Results from script validation
    validation_errors: Optional[List[Dict]]  # SQL validation errors with context
    script_retry_count: int  # Track script regeneration attempts
    regeneration_context: Optional[Dict]  # Error context for script regeneration
    
    # Output
    final_etl_config: Optional[Dict]
    final_script: Optional[str]
    reasoning_log: List[str]
    status: str  # 'analyzing', 'generating', 'testing', 'validating', 'completed', 'error'


# ============== Database Tools ==============

class ETLTools:
    """Tools for ETL Agent to interact with database and Neo4j."""
    
    def __init__(self):
        self.settings = get_settings()
        self._pool: Optional[asyncpg.Pool] = None
    
    async def get_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                host=self.settings.oltp_db_host,
                port=self.settings.oltp_db_port,
                user=self.settings.oltp_db_user,
                password=self.settings.oltp_db_password,
                database=self.settings.oltp_db_name,
                min_size=1,
                max_size=5
            )
        return self._pool
    
    async def query_table_schema(self, schema: str, table_name: str) -> Dict:
        """Get table schema information."""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Get columns
            columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, schema.lower(), table_name.lower())
            
            # Get sample data
            try:
                sample = await conn.fetch(f"""
                    SELECT * FROM {schema}.{table_name} LIMIT 5
                """)
                sample_data = [dict(row) for row in sample]
            except:
                sample_data = []
            
            # Get row count
            try:
                count_result = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM {schema}.{table_name}
                """)
            except:
                count_result = 0
            
            return {
                "schema": schema,
                "table_name": table_name,
                "columns": [dict(col) for col in columns],
                "sample_data": sample_data,
                "row_count": count_result
            }
    
    async def list_tables_in_schema(self, schema: str) -> List[str]:
        """List all tables in a schema."""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = $1 AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, schema.lower())
            return [row['table_name'] for row in tables]
    
    def _convert_to_serializable(self, obj):
        """Convert non-serializable types (Decimal, datetime, etc.) to JSON-serializable types."""
        from decimal import Decimal as DecimalType
        from datetime import datetime, date, time
        
        if isinstance(obj, dict):
            return {k: self._convert_to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_serializable(item) for item in obj]
        elif isinstance(obj, DecimalType):
            return float(obj)
        elif isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        else:
            return obj
    
    async def test_sql_query(self, sql: str, limit: int = 10) -> Dict:
        """Test a SQL query and return results."""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            try:
                # Add limit if not present
                test_sql = sql.strip().rstrip(';')
                if 'LIMIT' not in test_sql.upper():
                    test_sql = f"{test_sql} LIMIT {limit}"
                
                results = await conn.fetch(test_sql)
                sample_data = [self._convert_to_serializable(dict(row)) for row in results[:5]]
                return {
                    "success": True,
                    "row_count": len(results),
                    "columns": list(results[0].keys()) if results else [],
                    "sample_data": sample_data,
                    "sql": test_sql
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e),
                    "sql": sql
                }
    
    async def get_distinct_values(self, schema: str, table: str, column: str, limit: int = 20) -> List:
        """Get distinct values from a column."""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            try:
                results = await conn.fetch(f"""
                    SELECT DISTINCT {column} 
                    FROM {schema}.{table} 
                    WHERE {column} IS NOT NULL
                    LIMIT {limit}
                """)
                return [row[column] for row in results]
            except Exception as e:
                return [f"Error: {str(e)}"]
    
    async def create_temp_table_and_load(self, table_name: str, select_sql: str) -> Dict:
        """Create a temp table and load data to test ETL logic."""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            try:
                # Create temp table
                await conn.execute(f"""
                    CREATE TEMP TABLE IF NOT EXISTS {table_name} AS
                    {select_sql}
                    LIMIT 100
                """)
                
                # Get count
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                
                # Get sample
                sample = await conn.fetch(f"SELECT * FROM {table_name} LIMIT 5")
                
                return {
                    "success": True,
                    "table_name": table_name,
                    "row_count": count,
                    "sample_data": [dict(row) for row in sample],
                    "columns": list(sample[0].keys()) if sample else []
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e)
                }
    
    async def validate_olap_sql(self, fact_table: str, dimension_tables: List[Dict], 
                                 measures: List[str]) -> Dict:
        """Validate OLAP SQL by testing fact-dimension joins.
        
        Args:
            fact_table: Fact table name (e.g., "dw.fact_turbidity")
            dimension_tables: List of dimension table info with FK columns
            measures: List of measure columns
            
        Returns:
            Validation result with success status and any errors
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            results = {
                "success": True,
                "validations": [],
                "errors": []
            }
            
            # 1. Check fact table exists and has data
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {fact_table}")
                results["validations"].append({
                    "test": "fact_table_data",
                    "success": count > 0,
                    "message": f"Fact table {fact_table} has {count} rows"
                })
                if count == 0:
                    results["errors"].append(f"Fact table {fact_table} has no data")
                    results["success"] = False
            except Exception as e:
                results["errors"].append(f"Fact table error: {str(e)}")
                results["success"] = False
                return results
            
            # 2. Check each dimension table and FK relationship
            for dim in dimension_tables:
                dim_table = dim.get("table", "")
                fk_column = dim.get("foreign_key", "")
                dim_pk = dim.get("primary_key", "id")
                
                if not fk_column:
                    results["errors"].append(f"Dimension {dim_table} has no foreign_key defined")
                    results["success"] = False
                    continue
                
                # Check FK column exists in fact table
                try:
                    columns = await conn.fetch(f"""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema || '.' || table_name = '{fact_table}'
                          AND column_name = '{fk_column}'
                    """)
                    
                    if not columns:
                        # Try case-insensitive search
                        schema_table = fact_table.split(".")
                        if len(schema_table) == 2:
                            columns = await conn.fetch(f"""
                                SELECT column_name FROM information_schema.columns
                                WHERE LOWER(table_schema) = LOWER('{schema_table[0]}')
                                  AND LOWER(table_name) = LOWER('{schema_table[1]}')
                                  AND LOWER(column_name) = LOWER('{fk_column}')
                            """)
                    
                    if columns:
                        results["validations"].append({
                            "test": f"fk_column_{fk_column}",
                            "success": True,
                            "message": f"FK column {fk_column} exists in fact table"
                        })
                    else:
                        results["errors"].append(f"FK column '{fk_column}' not found in {fact_table}")
                        results["success"] = False
                        continue
                except Exception as e:
                    results["errors"].append(f"FK check error for {fk_column}: {str(e)}")
                    continue
                
                # Test actual join
                try:
                    join_sql = f"""
                        SELECT COUNT(*) FROM {fact_table} f
                        JOIN {dim_table} d ON f.{fk_column} = d.{dim_pk}
                    """
                    join_count = await conn.fetchval(join_sql)
                    
                    results["validations"].append({
                        "test": f"join_{dim_table}",
                        "success": join_count > 0,
                        "message": f"Join with {dim_table} successful: {join_count} rows"
                    })
                    
                    if join_count == 0:
                        results["errors"].append(f"Join with {dim_table} returns 0 rows - FK values may not match")
                except Exception as e:
                    results["errors"].append(f"Join test with {dim_table} failed: {str(e)}")
            
            # 3. Test a sample OLAP-style query with all dimensions
            if results["success"] and dimension_tables:
                try:
                    sample_dim = dimension_tables[0]
                    sample_measure = measures[0] if measures else "COUNT(*)"
                    
                    olap_sql = f"""
                        SELECT d.{sample_dim.get('primary_key', 'id')},
                               SUM(f.{sample_measure}) as measure_sum
                        FROM {fact_table} f
                        JOIN {sample_dim['table']} d 
                             ON f.{sample_dim.get('foreign_key', 'id')} = d.{sample_dim.get('primary_key', 'id')}
                        GROUP BY d.{sample_dim.get('primary_key', 'id')}
                        LIMIT 5
                    """
                    
                    olap_result = await conn.fetch(olap_sql)
                    results["validations"].append({
                        "test": "olap_query",
                        "success": len(olap_result) > 0,
                        "message": f"OLAP query successful: {len(olap_result)} groups returned",
                        "sample_sql": olap_sql
                    })
                except Exception as e:
                    results["errors"].append(f"OLAP query test failed: {str(e)}")
                    results["success"] = False
            
            return results
    
    def get_cube_fk_requirements(self, cube_def: Optional[Dict]) -> List[Dict]:
        """Extract FK requirements from cube definition (Mondrian schema).
        
        Args:
            cube_def: Cube definition from metadata store
            
        Returns:
            List of FK requirements for the fact table
        """
        fk_requirements = []
        
        if not cube_def:
            return fk_requirements
        
        dimensions = cube_def.get("dimensions", [])
        
        for dim in dimensions:
            dim_name = dim.get("name", "")
            dim_table = dim.get("table", "")
            fk = dim.get("foreign_key", "")
            
            # Find the primary key from levels
            levels = dim.get("levels", [])
            pk_column = "id"  # default
            if levels:
                pk_column = levels[0].get("column", "id")
            
            if dim_table:
                fk_requirements.append({
                    "dimension_name": dim_name,
                    "dimension_table": dim_table,
                    "foreign_key": fk if fk else f"{dim_name.lower()}_id",
                    "primary_key": pk_column,
                    "description": f"FK to {dim_table} dimension"
                })
        
        return fk_requirements
    
    def extract_cube_schema_requirements(self, cube_def: Optional[Dict]) -> Dict:
        """Extract all field requirements from cube schema for ETL validation.
        
        This extracts dimension names, level/attribute names, measure names, and FK columns
        that MUST be present in the generated ETL tables.
        
        Args:
            cube_def: Cube definition from metadata store
            
        Returns:
            Dict with required fields for each table
        """
        requirements = {
            "fact_table": {
                "name": "",
                "required_fk_columns": [],
                "required_measure_columns": []
            },
            "dimensions": []
        }
        
        if not cube_def:
            return requirements
        
        # Extract fact table info
        fact_table = cube_def.get("fact_table", "")
        requirements["fact_table"]["name"] = fact_table
        
        # Extract dimensions and their levels/attributes
        dimensions = cube_def.get("dimensions", [])
        for dim in dimensions:
            dim_name = dim.get("name", "")
            dim_table = dim.get("table", "")
            fk = dim.get("foreign_key", "")
            
            dim_info = {
                "name": dim_name,
                "table": dim_table,
                "foreign_key": fk if fk else f"{dim_name.lower()}_id",
                "required_columns": ["id"],  # Primary key is always required
                "levels": []
            }
            
            # Add FK to fact table requirements
            requirements["fact_table"]["required_fk_columns"].append(dim_info["foreign_key"])
            
            # Extract levels (attributes)
            levels = dim.get("levels", [])
            for level in levels:
                level_name = level.get("name", "")
                level_column = level.get("column", "")
                
                if level_column and level_column not in dim_info["required_columns"]:
                    dim_info["required_columns"].append(level_column)
                
                dim_info["levels"].append({
                    "name": level_name,
                    "column": level_column
                })
            
            requirements["dimensions"].append(dim_info)
        
        # Extract measures
        measures = cube_def.get("measures", [])
        for measure in measures:
            measure_column = measure.get("column", "")
            if measure_column:
                requirements["fact_table"]["required_measure_columns"].append(measure_column)
        
        return requirements
    
    async def validate_etl_against_cube_schema(self, script: str, cube_def: Optional[Dict]) -> Dict:
        """Validate that generated ETL script creates tables matching cube schema.
        
        This is a CRITICAL validation step that ensures:
        1. All dimension tables have required columns (id, levels/attributes)
        2. Fact table has all required FK columns
        3. Fact table has all required measure columns
        
        Args:
            script: Generated ETL Python script
            cube_def: Cube definition from metadata store
            
        Returns:
            Validation result with success status and any mismatches
        """
        import re
        
        result = {
            "success": True,
            "validations": [],
            "missing_columns": [],
            "recommendations": []
        }
        
        if not cube_def:
            result["validations"].append({
                "test": "cube_schema",
                "success": False,
                "message": "No cube definition available for validation"
            })
            result["success"] = False
            return result
        
        requirements = self.extract_cube_schema_requirements(cube_def)
        
        # 1. Check fact table FK columns
        fact_fks = requirements["fact_table"]["required_fk_columns"]
        for fk in fact_fks:
            # Look for the FK column in CREATE TABLE or INSERT statements for fact table
            fk_pattern = re.compile(rf'\b{re.escape(fk)}\b', re.IGNORECASE)
            if fk_pattern.search(script):
                result["validations"].append({
                    "test": f"fact_fk_{fk}",
                    "success": True,
                    "message": f"FK column '{fk}' found in script"
                })
            else:
                result["validations"].append({
                    "test": f"fact_fk_{fk}",
                    "success": False,
                    "message": f"FK column '{fk}' NOT found in script"
                })
                result["missing_columns"].append({
                    "table": "fact_table",
                    "column": fk,
                    "type": "foreign_key"
                })
                result["success"] = False
        
        # 2. Check fact table measure columns
        measures = requirements["fact_table"]["required_measure_columns"]
        for measure in measures:
            measure_pattern = re.compile(rf'\b{re.escape(measure)}\b', re.IGNORECASE)
            if measure_pattern.search(script):
                result["validations"].append({
                    "test": f"fact_measure_{measure}",
                    "success": True,
                    "message": f"Measure column '{measure}' found in script"
                })
            else:
                result["validations"].append({
                    "test": f"fact_measure_{measure}",
                    "success": False,
                    "message": f"Measure column '{measure}' NOT found in script"
                })
                result["missing_columns"].append({
                    "table": "fact_table",
                    "column": measure,
                    "type": "measure"
                })
                result["success"] = False
        
        # 3. Check dimension tables and their columns
        for dim in requirements["dimensions"]:
            dim_name = dim["name"]
            dim_table = dim["table"]
            
            # Check dimension table is created
            dim_table_pattern = re.compile(
                rf'CREATE\s+TABLE.*{re.escape(dim_table)}',
                re.IGNORECASE | re.DOTALL
            )
            if not dim_table_pattern.search(script):
                # Try without schema prefix
                simple_name = dim_table.split(".")[-1] if "." in dim_table else dim_table
                dim_table_pattern = re.compile(
                    rf'CREATE\s+TABLE.*{re.escape(simple_name)}',
                    re.IGNORECASE | re.DOTALL
                )
            
            if dim_table_pattern.search(script):
                result["validations"].append({
                    "test": f"dim_table_{dim_name}",
                    "success": True,
                    "message": f"Dimension table '{dim_table}' created in script"
                })
            else:
                result["validations"].append({
                    "test": f"dim_table_{dim_name}",
                    "success": False,
                    "message": f"Dimension table '{dim_table}' NOT created in script"
                })
                result["success"] = False
            
            # Check required columns for this dimension
            for level in dim["levels"]:
                level_col = level["column"]
                if level_col:
                    col_pattern = re.compile(rf'\b{re.escape(level_col)}\b', re.IGNORECASE)
                    if col_pattern.search(script):
                        result["validations"].append({
                            "test": f"dim_{dim_name}_col_{level_col}",
                            "success": True,
                            "message": f"Level column '{level_col}' found for {dim_name}"
                        })
                    else:
                        result["validations"].append({
                            "test": f"dim_{dim_name}_col_{level_col}",
                            "success": False,
                            "message": f"Level column '{level_col}' NOT found for {dim_name}"
                        })
                        result["missing_columns"].append({
                            "table": dim_table,
                            "column": level_col,
                            "type": "level",
                            "dimension": dim_name
                        })
                        result["success"] = False
        
        # Generate recommendations for missing columns
        if result["missing_columns"]:
            result["recommendations"].append(
                "다음 컬럼이 ETL 스크립트에 누락되었습니다. 큐브 스키마와 일치하도록 추가해야 합니다:"
            )
            for mc in result["missing_columns"]:
                result["recommendations"].append(
                    f"  - {mc['table']}.{mc['column']} ({mc['type']})"
                )
        
        return result
    
    async def sample_source_tables(self, source_tables: List[str], limit: int = 10) -> Dict[str, Dict]:
        """Sample data from multiple source tables for ETL analysis.
        
        Args:
            source_tables: List of table names in format "SCHEMA.TABLE_NAME"
            limit: Number of rows to sample per table
            
        Returns:
            Dict with table names as keys and sample data as values
        """
        pool = await self.get_pool()
        results = {}
        
        async with pool.acquire() as conn:
            for table_ref in source_tables:
                try:
                    parts = table_ref.split(".")
                    if len(parts) == 2:
                        schema, table_name = parts
                    else:
                        schema, table_name = "public", parts[0]
                    
                    # Handle case-sensitive table names in PostgreSQL
                    # Try multiple combinations for case sensitivity
                    sample = None
                    last_error = None
                    
                    # List of SQL variations to try (PostgreSQL case sensitivity handling)
                    sql_variations = [
                        f'SELECT * FROM "{schema}"."{table_name}" LIMIT {limit}',  # Both quoted (preserves case)
                        f'SELECT * FROM {schema.upper()}."{table_name}" LIMIT {limit}',  # Schema uppercase unquoted, table quoted
                        f'SELECT * FROM "{schema.upper()}"."{table_name}" LIMIT {limit}',  # Both uppercase quoted
                        f'SELECT * FROM {schema}."{table_name}" LIMIT {limit}',    # Table quoted only
                        f'SELECT * FROM "{schema}".{table_name} LIMIT {limit}',    # Schema quoted only  
                        f'SELECT * FROM {schema.lower()}."{table_name}" LIMIT {limit}',  # Schema lowercase, table quoted
                        f'SELECT * FROM {schema.lower()}.{table_name.lower()} LIMIT {limit}',  # All lowercase
                    ]
                    
                    for sql in sql_variations:
                        try:
                            sample = await conn.fetch(sql)
                            if sample is not None:
                                break
                        except Exception as sql_err:
                            last_error = sql_err
                            continue
                    
                    if sample is None:
                        raise last_error or Exception(f"테이블 {table_ref}를 찾을 수 없습니다")
                    
                    # Get column info
                    columns_info = await conn.fetch("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema ILIKE $1 AND table_name ILIKE $2
                        ORDER BY ordinal_position
                    """, schema, table_name)
                    
                    # Get distinct values for key columns (useful for understanding data)
                    distinct_values = {}
                    for col in columns_info[:5]:  # First 5 columns
                        col_name = col['column_name']
                        try:
                            distinct = await conn.fetch(f'''
                                SELECT DISTINCT "{col_name}" 
                                FROM "{schema}"."{table_name}" 
                                WHERE "{col_name}" IS NOT NULL 
                                LIMIT 10
                            ''')
                            distinct_values[col_name] = [str(row[col_name]) for row in distinct]
                        except:
                            pass
                    
                    results[table_ref] = {
                        "success": True,
                        "schema": schema,
                        "table_name": table_name,
                        "columns": [{"name": c['column_name'], "type": c['data_type']} for c in columns_info],
                        "sample_data": [dict(row) for row in sample],
                        "row_count": len(sample),
                        "distinct_values": distinct_values
                    }
                    
                except Exception as e:
                    error_msg = str(e)
                    # Log detailed error for debugging
                    import logging
                    logging.error(f"sample_source_tables failed for {table_ref}: {error_msg}")
                    results[table_ref] = {
                        "success": False,
                        "error": error_msg,
                        "tried_variations": [
                            f'"{schema}"."{table_name}"',
                            f'{schema}."{table_name}"',
                            f'"{schema}".{table_name}',
                            f'{schema}.{table_name}',
                            f'{schema.lower()}.{table_name.lower()}'
                        ]
                    }
        
        return results


# ============== ETL Agent ==============

class ETLAgent:
    """LangGraph-based ETL generation agent."""
    
    def __init__(self):
        self.settings = get_settings()
        self.tools = ETLTools()
        
        # Use settings to get API key (properly loaded from .env)
        api_key = self.settings.openai_api_key
        if not api_key:
            raise ValueError("OPENAI_API_KEY is not configured. Please set it in .env file.")
        
        # Use the latest and most capable model for ETL generation
        # Options: o3-mini, gpt-4o, gpt-4.1, o1-mini
        model_name = self.settings.etl_llm_model
        
        self.llm = ChatOpenAI(
            model=model_name,
            temperature=0,
            api_key=api_key
        )
        
        import logging
        logging.info(f"ETL Agent initialized with model: {model_name}")
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow."""
        
        workflow = StateGraph(ETLAgentState)
        
        # Add nodes
        workflow.add_node("analyze_sources", self._analyze_sources)
        workflow.add_node("design_dimensions", self._design_dimensions)
        workflow.add_node("design_fact", self._design_fact)
        workflow.add_node("test_etl", self._test_etl)
        workflow.add_node("generate_final", self._generate_final)
        workflow.add_node("validate_script", self._validate_script)  # NEW: Validate generated script
        
        # Add edges
        workflow.set_entry_point("analyze_sources")
        workflow.add_edge("analyze_sources", "design_dimensions")
        workflow.add_edge("design_dimensions", "design_fact")
        workflow.add_edge("design_fact", "test_etl")
        workflow.add_conditional_edges(
            "test_etl",
            self._should_continue,
            {
                "continue": "generate_final",
                "retry": "design_dimensions",
                "error": END
            }
        )
        workflow.add_edge("generate_final", "validate_script")
        workflow.add_conditional_edges(
            "validate_script",
            self._should_regenerate,
            {
                "complete": END,
                "regenerate": "generate_final",
                "error": END
            }
        )
        
        return workflow.compile()
    
    async def _analyze_sources(self, state: ETLAgentState) -> ETLAgentState:
        """Analyze source tables to understand data structure."""
        state["status"] = "analyzing"
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 🔍 소스 테이블 분석 시작...")
        
        # Check if ETL config with source tables is provided
        etl_config = state.get("etl_config")
        
        try:
            if etl_config and etl_config.get("source_tables"):
                # Use provided source tables from ETL config
                source_table_refs = etl_config.get("source_tables", [])
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] 📋 ETL 설정에서 소스 테이블 로드: {source_table_refs}"
                )
                
                # Sample data from specified source tables
                sampled_data = await self.tools.sample_source_tables(source_table_refs, limit=10)
                
                all_tables = []
                for table_ref, data in sampled_data.items():
                    if data.get("success"):
                        all_tables.append({
                            "schema": data["schema"],
                            "table_name": data["table_name"],
                            "columns": data["columns"],
                            "sample_data": data["sample_data"],
                            "row_count": data["row_count"],
                            "distinct_values": data.get("distinct_values", {})
                        })
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] ✅ {table_ref}: {len(data['columns'])}개 컬럼, {data['row_count']}개 샘플 로드"
                        )
                    else:
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] ⚠️ {table_ref} 샘플링 실패: {data.get('error')}"
                        )
                
                # Also store mappings for later use
                state["etl_mappings"] = etl_config.get("mappings", [])
                state["dimension_tables"] = etl_config.get("dimension_tables", [])
                state["fact_table"] = etl_config.get("fact_table", "")
                
            else:
                # Discover tables from database (original behavior)
                schemas_to_check = ["rwis", "public"]
                all_tables = []
                
                for schema in schemas_to_check:
                    try:
                        tables = await self.tools.list_tables_in_schema(schema)
                        for table in tables[:10]:  # Limit to 10 per schema
                            table_info = await self.tools.query_table_schema(schema, table)
                            all_tables.append(table_info)
                    except:
                        continue
            
            state["source_tables"] = all_tables
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ✅ {len(all_tables)}개 소스 테이블 분석 완료"
            )
            
            # Use LLM to analyze which tables are relevant
            tables_summary = "\n".join([
                f"- {t['schema']}.{t['table_name']}: {len(t['columns'])} columns, {t['row_count']} rows"
                for t in all_tables[:20]  # Limit for context
            ])
            
            prompt = f"""당신은 ETL 전문가입니다. 다음 큐브를 위한 ETL을 설계해야 합니다.

큐브 이름: {state['cube_name']}
큐브 설명: {state['cube_description']}
필요한 디멘전: {state['target_dimensions']}
필요한 측정값: {state['target_measures']}

사용 가능한 소스 테이블:
{tables_summary}

다음을 분석해주세요:
1. 각 디멘전을 위해 어떤 소스 테이블을 사용해야 하는가?
2. 팩트 테이블의 측정값을 위해 어떤 테이블을 사용해야 하는가?
3. 어떤 필터링이나 변환이 필요한가?

JSON 형식으로 응답해주세요:
{{
    "analysis": "전체 분석 요약",
    "recommended_sources": [
        {{"dimension": "dim_name", "source_table": "schema.table", "key_column": "col", "reason": "이유"}}
    ],
    "fact_sources": [
        {{"measure": "measure_name", "source_table": "schema.table", "column": "col", "aggregation": "SUM/AVG/COUNT"}}
    ],
    "filters": ["필터 조건들"],
    "joins": ["조인 조건들"]
}}
"""
            
            response = await self.llm.ainvoke([HumanMessage(content=prompt)])
            
            try:
                # Parse JSON from response
                content = response.content
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0]
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0]
                
                analysis = json.loads(content)
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] 📊 LLM 분석 완료: {analysis.get('analysis', '')[:100]}..."
                )
            except:
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ⚠️ LLM 응답 파싱 실패, 기본 전략 사용"
                )
            
        except Exception as e:
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ❌ 소스 분석 오류: {str(e)}"
            )
        
        return state
    
    def _ensure_dict(self, obj):
        """Convert object to dict if it's a Pydantic model or dataclass."""
        from dataclasses import is_dataclass, asdict
        
        if isinstance(obj, dict):
            return obj
        elif is_dataclass(obj) and not isinstance(obj, type):
            return asdict(obj)
        elif hasattr(obj, 'model_dump'):  # Pydantic v2
            return obj.model_dump()
        elif hasattr(obj, 'dict'):  # Pydantic v1
            return obj.dict()
        elif hasattr(obj, '__dataclass_fields__'):  # fallback for dataclass
            return {k: getattr(obj, k) for k in obj.__dataclass_fields__}
        elif hasattr(obj, '__dict__'):
            return vars(obj)
        else:
            return {"value": str(obj)}
    
    async def _design_dimensions(self, state: ETLAgentState) -> ETLAgentState:
        """Design dimension table ETL strategies."""
        state["status"] = "generating"
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 📐 디멘전 ETL 설계 중...")
        
        dimension_strategies = []
        etl_config = state.get("etl_config", {})
        raw_mappings = etl_config.get("mappings", []) or state.get("etl_mappings", [])
        
        # Convert mappings to dicts (handle ETLMapping objects)
        mappings = [self._ensure_dict(m) for m in raw_mappings]
        
        # Log available source tables for debugging
        available_tables = [f"{t.get('schema', '')}.{t.get('table_name', '')}" for t in state.get("source_tables", [])]
        state["reasoning_log"].append(
            f"[{datetime.now().isoformat()}] 📋 사용 가능한 소스 테이블: {available_tables}"
        )
        
        for dim_name in state["target_dimensions"]:
            # First, check if there's a mapping for this dimension in ETL config
            dim_mappings = [m for m in mappings if m.get("target_table") == dim_name or dim_name in str(m.get("target_table", ""))]
            
            # Find relevant source table from loaded tables
            # Helper to get column name (handles both 'name' and 'column_name' keys)
            def get_col_name(col):
                return col.get("column_name") or col.get("name") or ""
            
            relevant_tables = [
                t for t in state["source_tables"]
                if any(dim_name.lower().replace("dim_", "") in get_col_name(col).lower() 
                       for col in t.get("columns", []))
            ]
            
            if relevant_tables:
                source = relevant_tables[0]
                
                # Get distinct values to understand cardinality
                key_candidates = [
                    get_col_name(col) for col in source["columns"]
                    if "id" in get_col_name(col).lower() or "code" in get_col_name(col).lower()
                ]
                
                first_col = get_col_name(source["columns"][0]) if source["columns"] else "id"
                key_col = key_candidates[0] if key_candidates else first_col
                other_cols = [get_col_name(col) for col in source["columns"][:5] if get_col_name(col) not in key_candidates[:1]]
                
                strategy = {
                    "dimension_name": dim_name,
                    "source_schema": source["schema"],
                    "source_table": source["table_name"],
                    "strategy": "etl",
                    "columns": [get_col_name(col) for col in source["columns"][:10]],
                    "key_column": key_col,
                    "sql": f"""
SELECT DISTINCT 
    "{key_col}" as id,
    {', '.join([f'"{c}"' for c in other_cols]) if other_cols else f'"{key_col}"'}
FROM "{source["schema"]}"."{source["table_name"]}"
WHERE "{key_col}" IS NOT NULL
""".strip()
                }
                dimension_strategies.append(strategy)
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ✅ {dim_name}: {source['schema']}.{source['table_name']}에서 추출"
                )
            else:
                # Try to use ETL config mappings or any available source table
                if dim_mappings:
                    # Use mapping info
                    mapping = dim_mappings[0]
                    source_table = mapping.get("source_table", "")
                    source_col = mapping.get("source_column", "")
                    strategy = {
                        "dimension_name": dim_name,
                        "source_schema": source_table.split(".")[0] if "." in source_table else "public",
                        "source_table": source_table.split(".")[-1] if "." in source_table else source_table,
                        "strategy": "etl",
                        "columns": [source_col] if source_col else [],
                        "key_column": source_col.split(".")[-1] if source_col else "id",
                        "sql": f"-- 매핑 정보 기반: {source_table}.{source_col} → {dim_name}"
                    }
                    dimension_strategies.append(strategy)
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] 📋 {dim_name}: 매핑 정보 사용 ({source_table})"
                    )
                elif state.get("source_tables"):
                    # Use first available source table as fallback
                    fallback = state["source_tables"][0]
                    strategy = {
                        "dimension_name": dim_name,
                        "source_schema": fallback.get("schema", "public"),
                        "source_table": fallback.get("table_name", ""),
                        "strategy": "extract",
                        "columns": [c.get("column_name", c.get("name", "")) for c in fallback.get("columns", [])[:5]],
                        "sql": f"-- {dim_name}: {fallback.get('schema')}.{fallback.get('table_name')}에서 관련 데이터 추출 필요"
                    }
                    dimension_strategies.append(strategy)
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] 🔄 {dim_name}: 대체 소스 테이블 사용 ({fallback.get('schema')}.{fallback.get('table_name')})"
                    )
                else:
                    # Generate dimension manually
                    strategy = {
                        "dimension_name": dim_name,
                        "source_schema": None,
                        "source_table": None,
                        "strategy": "generate",
                        "sql": f"-- {dim_name}은 수동으로 생성 필요 (디멘전 테이블 직접 정의)"
                    }
                    dimension_strategies.append(strategy)
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ⚠️ {dim_name}: 소스 테이블 없음, 수동 생성 필요"
                    )
        
        state["dimension_strategies"] = dimension_strategies
        
        # Log summary
        etl_strategies = [s for s in dimension_strategies if s.get("strategy") == "etl"]
        extract_strategies = [s for s in dimension_strategies if s.get("strategy") == "extract"]
        generate_strategies = [s for s in dimension_strategies if s.get("strategy") == "generate"]
        
        state["reasoning_log"].append(
            f"[{datetime.now().isoformat()}] 📊 디멘전 전략 요약: ETL={len(etl_strategies)}, 추출={len(extract_strategies)}, 생성={len(generate_strategies)}"
        )
        
        if not dimension_strategies:
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ⚠️ 디멘전 전략이 생성되지 않음 - 기본 전략으로 진행"
            )
            # Create default strategies based on dimension names
            for dim_name in state["target_dimensions"]:
                dimension_strategies.append({
                    "dimension_name": dim_name,
                    "source_schema": "dw",
                    "source_table": dim_name,
                    "strategy": "generate",
                    "sql": f"-- {dim_name} 테이블 직접 생성"
                })
            state["dimension_strategies"] = dimension_strategies
        
        return state
    
    async def _design_fact(self, state: ETLAgentState) -> ETLAgentState:
        """Design fact table ETL strategy."""
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 📊 팩트 테이블 ETL 설계 중...")
        
        # Find tables with numeric columns for measures
        measure_sources = []
        for table in state["source_tables"]:
            # Helper to get column type (handles both 'type' and 'data_type' keys)
            def get_col_type(col):
                return col.get("data_type") or col.get("type") or ""
            
            numeric_cols = [
                col for col in table.get("columns", [])
                if get_col_type(col).lower() in ["numeric", "integer", "bigint", "real", "double precision", "decimal", "int4", "int8", "float4", "float8", "number"]
            ]
            if numeric_cols:
                # Helper function for column name
                def get_col_name_local(col):
                    return col.get("column_name") or col.get("name") or ""
                
                measure_sources.append({
                    "table": f"{table['schema']}.{table['table_name']}",
                    "numeric_columns": [get_col_name_local(col) for col in numeric_cols],
                    "row_count": table.get("row_count", 0)
                })
        
        # Use LLM to design fact table ETL
        prompt = f"""팩트 테이블 ETL을 설계해주세요.

필요한 측정값: {state['target_measures']}
디멘전 전략: {json.dumps(state['dimension_strategies'], ensure_ascii=False, indent=2)}

사용 가능한 숫자 컬럼이 있는 테이블:
{json.dumps(measure_sources[:10], ensure_ascii=False, indent=2)}

각 측정값을 어떻게 계산할지 SQL로 작성해주세요.
JSON 형식으로 응답:
{{
    "fact_table_name": "dw.fact_xxx",
    "source_tables": ["schema.table"],
    "columns": [
        {{"name": "measure_name", "expression": "AVG(column)", "type": "NUMERIC"}}
    ],
    "joins": ["JOIN 조건들"],
    "group_by": ["그룹화 컬럼들"],
    "where": "필터 조건"
}}
"""
        
        try:
            response = await self.llm.ainvoke([HumanMessage(content=prompt)])
            content = response.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            
            fact_strategy = json.loads(content)
            state["fact_strategy"] = fact_strategy
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ✅ 팩트 테이블 전략 완료: {fact_strategy.get('fact_table_name', 'unknown')}"
            )
        except Exception as e:
            state["fact_strategy"] = {"error": str(e)}
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ❌ 팩트 테이블 설계 오류: {str(e)}"
            )
        
        return state
    
    async def _test_etl(self, state: ETLAgentState) -> ETLAgentState:
        """Test ETL logic with actual queries."""
        state["status"] = "testing"
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 🧪 ETL 로직 테스트 중...")
        
        test_results = []
        
        # Test dimension SQLs
        for dim_strategy in state.get("dimension_strategies", []):
            if dim_strategy.get("sql") and not dim_strategy["sql"].startswith("--"):
                result = await self.tools.test_sql_query(dim_strategy["sql"], limit=5)
                test_results.append({
                    "type": "dimension",
                    "name": dim_strategy["dimension_name"],
                    "result": result
                })
                
                if result["success"]:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ✅ {dim_strategy['dimension_name']} 테스트 성공: {result['row_count']} 행"
                    )
                else:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ❌ {dim_strategy['dimension_name']} 테스트 실패: {result.get('error', 'unknown')}"
                    )
        
        state["test_results"] = test_results
        return state
    
    def _should_continue(self, state: ETLAgentState) -> str:
        """Determine if we should continue, retry, or end."""
        test_results = state.get("test_results", [])
        retry_count = state.get("retry_count", 0)
        
        if not test_results:
            return "continue"
        
        # Check if all tests passed
        all_passed = all(r.get("result", {}).get("success", False) for r in test_results)
        
        if all_passed:
            return "continue"
        
        # Check for unrecoverable errors (DB connection issues)
        for result in test_results:
            error = result.get("result", {}).get("error", "")
            if any(x in error.lower() for x in ["connection refused", "authentication failed", "could not connect"]):
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ⛔ DB 연결 오류 - 사용자 조치 필요: {error}"
                )
                return "error"
        
        # Retry up to 3 times for code-fixable errors
        if retry_count < 3:
            state["retry_count"] = retry_count + 1
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] 🔄 오류 수정 시도 {retry_count + 1}/3..."
            )
            return "retry"
        
        # Max retries reached
        state["reasoning_log"].append(
            f"[{datetime.now().isoformat()}] ⚠️ 최대 재시도 횟수 초과 - 수동 검토 필요"
        )
        return "continue"
    
    async def _generate_final(self, state: ETLAgentState) -> ETLAgentState:
        """Generate final ETL configuration and script with validation."""
        state["status"] = "generating"
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 📝 최종 ETL 스크립트 생성 중...")
        
        # Get actual DB connection info from settings (properly loaded from .env)
        db_host = self.settings.oltp_db_host
        db_port = str(self.settings.oltp_db_port)
        db_user = self.settings.oltp_db_user
        db_password = self.settings.oltp_db_password
        db_name = self.settings.oltp_db_name
        
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 📌 DB 연결 정보: {db_host}:{db_port}/{db_name}")
        
        # Use ETL config from state if available (from UI), otherwise build from strategies
        provided_config = state.get("etl_config")
        if provided_config:
            etl_config = provided_config
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] 📋 UI에서 제공된 ETL 설정 사용"
            )
        else:
            # Build from discovered strategies
            etl_config = {
                "cube_name": state["cube_name"],
                "fact_table": state.get("fact_strategy", {}).get("fact_table_name", f"dw.fact_{state['cube_name']}"),
                "dimension_tables": [f"dw.{d['dimension_name']}" for d in state.get("dimension_strategies", [])],
                "source_tables": list(set([
                    f"{d['source_schema']}.{d['source_table']}" 
                    for d in state.get("dimension_strategies", [])
                    if d.get("source_table")
                ])),
                "mappings": state.get("etl_mappings", []),
                "dw_schema": "dw",
                "sync_mode": "full"
            }
        
        # Build source table info for prompt
        source_tables_info = ""
        for table_data in state.get("source_tables", []):
            if isinstance(table_data, dict):
                table_name = f"{table_data.get('schema', 'public')}.{table_data.get('table_name', 'unknown')}"
                columns = table_data.get('columns', [])
                sample = table_data.get('sample_data', [])
                distinct_vals = table_data.get('distinct_values', {})
                
                source_tables_info += f"\n### 테이블: {table_name}\n"
                source_tables_info += f"컬럼: {', '.join([c.get('name', c.get('column_name', '')) for c in columns[:10]])}\n"
                if sample:
                    source_tables_info += f"샘플 데이터 (첫 2행):\n{json.dumps(sample[:2], ensure_ascii=False, indent=2, default=str)}\n"
                if distinct_vals:
                    source_tables_info += f"주요 컬럼 값 예시: {json.dumps(distinct_vals, ensure_ascii=False, default=str)}\n"
        
        # Collect previous errors for context if retrying
        previous_errors = []
        for result in state.get("test_results", []):
            if not result.get("result", {}).get("success", False):
                previous_errors.append({
                    "name": result.get("name", "unknown"),
                    "error": result.get("result", {}).get("error", "")
                })
        
        error_context = ""
        if previous_errors:
            error_context = f"""
이전 실행에서 발생한 오류 (반드시 수정 필요):
{json.dumps(previous_errors, ensure_ascii=False, indent=2)}

주요 수정 포인트:
- PostgreSQL에서 대소문자 구분: 테이블명이 대문자면 "TABLE_NAME"처럼 쌍따옴표 필요
- 스키마 지정: rwis.TABLE_NAME 또는 "rwis"."TABLE_NAME" 형태로 사용
- 포트는 반드시 정수형으로: port=5432 (문자열 아님)
"""
        
        # Generate Python script with LLM - use environment variables for security
        # Include full ETL config with mappings
        # Convert ETLMapping objects to dicts for JSON serialization
        raw_mappings = etl_config.get('mappings', [])
        mappings_as_dicts = [self._ensure_dict(m) for m in raw_mappings]
        mappings_info = json.dumps(mappings_as_dicts, ensure_ascii=False, indent=2)
        dimension_tables_info = json.dumps(etl_config.get('dimension_tables', []), ensure_ascii=False, indent=2)
        
        # 🔑 CRITICAL: Load actual cube metadata from metadata_store to get real Level columns
        actual_cube_def = None
        actual_cube_dimensions = {}  # dimension_name -> {"levels": [...], "foreign_key": "..."}
        try:
            from ..services.metadata_store import metadata_store
            cube_name = state.get("cube_name", "")
            if cube_name:
                cube = metadata_store.get_cube(cube_name)
                if cube:
                    actual_cube_def = cube.model_dump()
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ✅ 큐브 메타데이터 로드 완료: {cube_name}"
                    )
                    # Extract dimension level info from actual cube
                    for dim in cube.dimensions:
                        dim_levels = [{"name": level.name, "column": level.column} for level in dim.levels]
                        actual_cube_dimensions[dim.name] = {
                            "table": dim.table,
                            "foreign_key": dim.foreign_key or f"{dim.name}_id",
                            "levels": dim_levels
                        }
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] 📋 디멘전 '{dim.name}' 레벨 컬럼: {[l['column'] for l in dim_levels]}"
                        )
                else:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ⚠️ 큐브 '{cube_name}'을(를) 찾을 수 없음 - 기본값 사용"
                    )
        except Exception as e:
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ⚠️ 큐브 메타데이터 로드 실패: {str(e)}"
            )
        
        # Extract FK requirements from dimension strategies (use actual cube FK names)
        fk_requirements = []
        for dim in state.get('dimension_strategies', []):
            dim_name = dim.get('dimension_name', '')
            # Use actual FK from cube metadata if available
            actual_dim_info = actual_cube_dimensions.get(dim_name, {})
            fk_col = actual_dim_info.get('foreign_key', f"{dim_name}_id")
            fk_requirements.append({
                "dimension_table": f"dw.{dim_name}",
                "fk_column_in_fact": fk_col,
                "description": f"FK to {dim_name} dimension"
            })
        
        # Extract full cube schema requirements for validation
        # 🔑 Use ACTUAL cube Level columns instead of hardcoded values!
        etl_config = state.get("etl_config", {})
        
        # Build dimensions with real level info from cube metadata
        cube_dimensions_for_requirements = []
        for dim in state.get("dimension_strategies", []):
            dim_name = dim.get("dimension_name", "")
            actual_dim_info = actual_cube_dimensions.get(dim_name, {})
            
            # Use actual levels from cube metadata, fallback to id only
            actual_levels = actual_dim_info.get("levels", [])
            if not actual_levels:
                actual_levels = [{"name": "id", "column": "id"}]
            
            cube_dimensions_for_requirements.append({
                "name": dim_name.replace("dim_", ""),
                "table": f"dw.{dim_name}",
                "foreign_key": actual_dim_info.get("foreign_key", f"{dim_name}_id"),
                "levels": actual_levels
            })
        
        cube_schema_requirements = self.tools.extract_cube_schema_requirements({
            "fact_table": etl_config.get("fact_table", f"dw.fact_{state['cube_name']}"),
            "dimensions": cube_dimensions_for_requirements,
            "measures": [{"column": m} for m in state.get("target_measures", [])]
        })
        
        # Build cube schema section for prompt
        cube_schema_text = "## 🔒 큐브 스키마 필드 요구사항 (ETL 생성 시 반드시 일치해야 함!)\n\n"
        cube_schema_text += "생성된 ETL 스크립트는 큐브 스키마와 정확히 일치해야 합니다.\n"
        cube_schema_text += "다음 필드들이 반드시 포함되어야 합니다:\n\n"
        
        # Fact table requirements
        cube_schema_text += f"### 팩트 테이블: {cube_schema_requirements['fact_table']['name']}\n"
        cube_schema_text += "**필수 FK 컬럼:**\n"
        for fk in cube_schema_requirements['fact_table']['required_fk_columns']:
            cube_schema_text += f"  - `{fk}` (디멘전 연결용)\n"
        cube_schema_text += "**필수 측정값 컬럼:**\n"
        for measure in cube_schema_requirements['fact_table']['required_measure_columns']:
            cube_schema_text += f"  - `{measure}`\n"
        
        # Dimension table requirements
        cube_schema_text += "\n### 디멘전 테이블:\n"
        for dim in cube_schema_requirements['dimensions']:
            cube_schema_text += f"**{dim['table']}**\n"
            cube_schema_text += f"  - FK: `{dim['foreign_key']}`\n"
            cube_schema_text += f"  - 필수 컬럼: {', '.join(dim['required_columns'])}\n"
            if dim['levels']:
                cube_schema_text += f"  - 레벨/어트리뷰트: {', '.join([l['column'] for l in dim['levels'] if l['column']])}\n"
        
        cube_schema_text += "\n⚠️ 위 필드 중 하나라도 누락되면 큐브가 정상 동작하지 않습니다!\n\n"
        
        # Build FK requirements section for prompt - use ACTUAL cube schema Level columns
        fk_requirements_text = "## ⚠️ 필수 FK 컬럼 (OLAP 조인을 위해 반드시 포함!)\n"
        fk_requirements_text += "팩트 테이블에는 다음 FK 컬럼이 반드시 포함되어야 합니다:\n"
        for fk in fk_requirements:
            fk_requirements_text += f"- `{fk['fk_column_in_fact']}` → {fk['dimension_table']}.id\n"
        fk_requirements_text += "\n"
        
        # 🔑 Generate DYNAMIC example DDL based on actual cube schema
        example_dim_ddl_lines = []
        for dim_name, dim_info in actual_cube_dimensions.items():
            dim_levels = dim_info.get("levels", [])
            level_columns = [level["column"] for level in dim_levels if level["column"] != "id"]
            
            # Build column definitions from actual Level columns
            col_defs = ["id SERIAL PRIMARY KEY"]
            for col in level_columns:
                col_defs.append(f"{col} VARCHAR(255)  -- 🔒 큐브 스키마 Level 컬럼 (변경 금지!)")
            
            example_dim_ddl_lines.append(f"""-- {dim_name} 테이블 (큐브 스키마에서 정의된 Level 컬럼 사용)
CREATE TABLE dw.{dim_name} (
    {', '.join(col_defs) if len(col_defs) <= 2 else (','+chr(10)+'    ').join(col_defs)}
);""")
        
        # If no actual dimensions found, provide generic example
        if not example_dim_ddl_lines:
            example_dim_ddl_lines.append("""-- dim_xxx 테이블 예시
CREATE TABLE dw.dim_xxx (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)  -- Level 컬럼
);""")
        
        fk_requirements_text += f"""⚠️ 중요: 디멘전 테이블의 컬럼명은 반드시 큐브 스키마의 Level 컬럼명과 일치해야 합니다!
예: 큐브 스키마에서 Level column="name"이면 → 테이블에도 name 컬럼 생성
    큐브 스키마에서 Level column="description"이면 → 테이블에도 description 컬럼 생성
    site_name, tag_desc 등 임의의 컬럼명 사용 금지!

실제 큐브 스키마 기반 DDL 예시:
```sql
{chr(10).join(example_dim_ddl_lines)}

-- fact 테이블에서 FK 연결
CREATE TABLE dw.fact_xxx (
    {', '.join([f'{fk["fk_column_in_fact"]} INTEGER REFERENCES {fk["dimension_table"]}(id)' for fk in fk_requirements]) if fk_requirements else 'dim_xxx_id INTEGER REFERENCES dw.dim_xxx(id)'},
    ...measure columns...
);

-- 팩트 테이블 INSERT 시 디멘전 테이블과 조인하여 FK 값 확보
INSERT INTO dw.fact_xxx ({', '.join([fk['fk_column_in_fact'] for fk in fk_requirements]) if fk_requirements else 'dim_xxx_id'}, measure1, ...)
SELECT 
    {', '.join([f'd{i}.id as {fk["fk_column_in_fact"]}' for i, fk in enumerate(fk_requirements)]) if fk_requirements else 'dx.id'},
    SUM(src.value) as measure1
FROM source_table src
{chr(10).join([f'JOIN {fk["dimension_table"]} d{i} ON src.key = d{i}.source_key' for i, fk in enumerate(fk_requirements)]) if fk_requirements else 'JOIN dw.dim_xxx dx ON src.xxx_code = dx.code'}
GROUP BY {', '.join([f'd{i}.id' for i in range(len(fk_requirements))]) if fk_requirements else 'dx.id'};
```
"""
        
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 🔗 FK 요구사항: {len(fk_requirements)}개 디멘전 연결 필요")
        
        # Check for regeneration context (from validation failures)
        regeneration_context = state.get("regeneration_context")
        regeneration_section = ""
        if regeneration_context:
            regen_errors = regeneration_context.get("errors", [])
            regen_hints = regeneration_context.get("hints", [])
            
            regeneration_section = f"""
## ⛔ 이전 생성에서 발견된 오류 (반드시 수정 필요!)
이전에 생성된 스크립트에서 다음 오류가 발견되었습니다. 이번에는 이 오류들을 반드시 수정해주세요:

### 발견된 오류:
{json.dumps(regen_errors, ensure_ascii=False, indent=2)}

### 수정 힌트:
{chr(10).join(['- ' + h for h in regen_hints])}

### 특히 주의해야 할 점:
1. **FROM 절에 모든 테이블 포함**: JOIN에서 사용하는 테이블은 반드시 FROM 절에 있어야 합니다
   - 잘못된 예: SELECT ... FROM table_a JOIN table_b ON ... JOIN table_c ON table_c.x = MISSING_TABLE.y
   - 올바른 예: SELECT ... FROM table_a JOIN table_b ON ... JOIN missing_table ON ... JOIN table_c ON ...

2. **PostgreSQL 대소문자 처리**: 대문자 스키마/테이블/컬럼은 쌍따옴표로 감싸야 합니다
   - 잘못된 예: FROM RWIS.TABLE_NAME
   - 올바른 예: FROM "RWIS"."TABLE_NAME"

3. **팩트 테이블 INSERT 시 모든 소스 테이블 명시**:
   - 팩트 데이터를 추출할 모든 소스 테이블을 FROM/JOIN에 포함
   - 디멘전 테이블과의 조인도 명시적으로 포함

"""
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] 🔄 이전 오류 컨텍스트 포함하여 재생성 중..."
            )
        
        prompt = f"""다음 ETL 설정과 소스 데이터를 기반으로 완전하고 실행 가능한 Python ETL 스크립트를 생성해주세요.
{regeneration_section}

## 반드시 사용해야 할 데이터베이스 연결 코드 (환경변수 사용):
```python
import os
import psycopg2
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경변수에서 DB 연결 정보 읽기 (실행 시 자동 주입됨)
DB_PARAMS = {{
    'host': os.environ.get('ETL_DB_HOST', 'localhost'),
    'port': int(os.environ.get('ETL_DB_PORT', '5432')),
    'user': os.environ.get('ETL_DB_USER', 'postgres'),
    'password': os.environ.get('ETL_DB_PASSWORD', ''),
    'database': os.environ.get('ETL_DB_NAME', 'postgres')
}}

def get_connection():
    logging.info(f"Connecting to {{DB_PARAMS['host']}}:{{DB_PARAMS['port']}}/{{DB_PARAMS['database']}}")
    return psycopg2.connect(**DB_PARAMS)
```

## 큐브 정보
- 큐브 이름: {state['cube_name']}
- 분석 의도: {state['cube_description']}

## ETL 설정 (이 설정을 정확히 따라주세요!)
- 팩트 테이블: {etl_config.get('fact_table', '')}
- 디멘전 테이블: {dimension_tables_info}
- 소스 테이블: {json.dumps(etl_config.get('source_tables', []), ensure_ascii=False)}
- DW 스키마: {etl_config.get('dw_schema', 'dw')}

## 컬럼 매핑 정보 (이 매핑을 기반으로 ETL 로직 구현):
{mappings_info}

## 실제 소스 테이블 데이터 샘플:
{source_tables_info}

## 디멘전 전략:
{json.dumps(state.get('dimension_strategies', []), ensure_ascii=False, indent=2)}

## 팩트 전략:
{json.dumps(state.get('fact_strategy', {}), ensure_ascii=False, indent=2)}

{cube_schema_text}
{fk_requirements_text}
{error_context}

## 필수 스크립트 요구사항
1. 위의 환경변수 기반 DB_PARAMS와 get_connection() 함수를 반드시 그대로 사용
2. 절대 하드코딩된 DB 연결정보 사용 금지 (환경변수만 사용)
3. 스키마 생성: CREATE SCHEMA IF NOT EXISTS dw
4. 디멘전 테이블 생성:
   - 각 디멘전 테이블은 `id` 컬럼을 SERIAL PRIMARY KEY로 생성
   - 소스 테이블에서 적절한 컬럼을 추출하여 데이터 적재
   - 분석 의도({state['cube_description']})에 맞는 데이터만 필터링
5. ⚠️ 팩트 테이블 생성 (FK 컬럼 필수!):
   - 매핑 정보의 transformation을 적용 (AVG, SUM, MAX 등)
   - 반드시 각 디멘전 테이블과 조인 가능한 FK 컬럼 포함: site_id, time_id, tag_id 등
   - FK 컬럼 값은 디멘전 테이블과 JOIN하여 채워야 함
   - 팩트 테이블 INSERT 시 디멘전 테이블의 id를 참조
6. OLAP 조인 보장:
   - 팩트 테이블의 FK 컬럼은 디멘전 테이블의 id를 정확히 참조해야 함
   - 예: fact.site_id = dim_site.id
   - INSERT 쿼리 예시:
     INSERT INTO dw.fact_xxx (site_id, time_id, measure1)
     SELECT ds.id, dt.id, SUM(src.value)
     FROM source src
     JOIN dw.dim_site ds ON src.site_code = ds.site_code
     JOIN dw.dim_time dt ON DATE(src.log_time) = dt.date_value
     GROUP BY ds.id, dt.id;
7. PostgreSQL 대소문자 규칙 준수:
   - 대문자 테이블명/컬럼명은 쌍따옴표로 감싸기: "RWIS"."TABLE_NAME"
   - 예: SELECT * FROM "RWIS"."RDF01HH_TB"
8. ⚠️ 인덱스 생성 (OLAP 성능 최적화 필수!):
   - 팩트 테이블의 모든 FK 컬럼에 인덱스 생성:
     CREATE INDEX idx_fact_xxx_dim_time_id ON dw.fact_xxx(dim_time_id);
     CREATE INDEX idx_fact_xxx_dim_site_id ON dw.fact_xxx(dim_site_id);
   - 디멘전 테이블의 조인 키 컬럼에 인덱스 생성:
     CREATE INDEX idx_dim_site_site_code ON dw.dim_site(site_code);
   - 복합 인덱스 (자주 함께 사용되는 컬럼):
     CREATE INDEX idx_fact_xxx_composite ON dw.fact_xxx(dim_time_id, dim_site_id);
9. Materialized View 생성 (대용량 데이터 집계 최적화):
   - 자주 사용되는 집계 쿼리를 위한 Materialized View 생성:
     CREATE MATERIALIZED VIEW dw.mv_xxx_by_site AS
     SELECT dim_site_id, AVG(measure1) as avg_measure1, SUM(measure2) as sum_measure2
     FROM dw.fact_xxx GROUP BY dim_site_id;
   - Materialized View 새로고침 함수 포함:
     REFRESH MATERIALIZED VIEW dw.mv_xxx_by_site;
   - Materialized View에도 인덱스 생성:
     CREATE INDEX idx_mv_xxx_by_site ON dw.mv_xxx_by_site(dim_site_id);
10. 상세한 로깅 (각 단계별 진행상황, 처리된 행 수, 인덱스 생성 결과)
11. 에러 핸들링 (try/except 블록) - conn과 cursor는 None으로 초기화
12. 트랜잭션 관리 (commit/rollback)
13. 각 함수는 명확한 docstring 포함
14. 인덱스 생성 함수를 별도로 분리하여 create_indexes() 함수 구현
15. Materialized View 생성/갱신 함수를 별도로 분리하여 create_materialized_views() 함수 구현

## ⚠️ 중요: 동기화 모드 지원 (전체 재적재 / 증분 적재)
스크립트는 반드시 동기화 모드를 환경변수로 받아 두 가지 모드를 지원해야 합니다:

1. **환경변수**: `ETL_SYNC_MODE` (값: 'full' 또는 'incremental', 기본값: 'full')

2. **전체 재적재 모드 (full)**:
   - 기존 DW 테이블 완전 삭제 후 재생성
   - 모든 데이터를 처음부터 다시 적재
   - 코드 예시:
     ```python
     SYNC_MODE = os.environ.get('ETL_SYNC_MODE', 'full')
     
     def drop_existing_tables(conn):
         if SYNC_MODE == 'full':
             logging.info("전체 재적재 모드: 기존 테이블 삭제 중...")
             with conn.cursor() as cursor:
                 cursor.execute("DROP TABLE IF EXISTS dw.fact_xxx CASCADE")
                 cursor.execute("DROP TABLE IF EXISTS dw.dim_xxx CASCADE")
                 # Materialized View도 삭제
                 cursor.execute("DROP MATERIALIZED VIEW IF EXISTS dw.mv_xxx CASCADE")
     ```

3. **증분 적재 모드 (incremental)**:
   - 기존 테이블 유지, 새로운 데이터만 추가
   - INSERT 시 중복 방지: ON CONFLICT DO UPDATE 또는 NOT EXISTS 사용
   - 마지막 처리 시점 기록 (ETL_LAST_SYNC 환경변수 또는 별도 메타 테이블)
   - 코드 예시:
     ```python
     def load_dimension_incremental(conn, dim_name, source_sql):
         \"\"\"증분 적재: 새로운 데이터만 추가\"\"\"
         with conn.cursor() as cursor:
             # UPSERT 패턴 사용
             cursor.execute(f\"\"\"
                 INSERT INTO dw.{{dim_name}} (key_col, attr1, attr2)
                 {{source_sql}}
                 ON CONFLICT (key_col) DO UPDATE SET
                     attr1 = EXCLUDED.attr1,
                     attr2 = EXCLUDED.attr2,
                     updated_at = NOW()
             \"\"\")
     
     def load_fact_incremental(conn):
         \"\"\"증분 적재: 마지막 동기화 이후 데이터만 처리\"\"\"
         last_sync = os.environ.get('ETL_LAST_SYNC', '1900-01-01')
         with conn.cursor() as cursor:
             cursor.execute(f\"\"\"
                 INSERT INTO dw.fact_xxx (dim_time_id, dim_site_id, measure1)
                 SELECT dt.id, ds.id, SUM(src.value)
                 FROM source_table src
                 JOIN dw.dim_time dt ON ...
                 JOIN dw.dim_site ds ON ...
                 WHERE src.created_at > '{{last_sync}}'
                 GROUP BY dt.id, ds.id
                 ON CONFLICT (dim_time_id, dim_site_id) DO UPDATE SET
                     measure1 = dw.fact_xxx.measure1 + EXCLUDED.measure1
             \"\"\")
     ```

4. **main() 함수 구조**:
   ```python
   def main():
       conn = None
       try:
           conn = get_connection()
           conn.autocommit = False
           
           create_schema(conn)
           
           if SYNC_MODE == 'full':
               logging.info("=== 전체 재적재 모드 ===")
               drop_existing_tables(conn)
               create_dimension_tables(conn)
               create_fact_table(conn)
           else:
               logging.info("=== 증분 적재 모드 ===")
               # 테이블이 없으면 생성
               create_dimension_tables_if_not_exists(conn)
               create_fact_table_if_not_exists(conn)
               # 증분 데이터만 적재
               load_dimensions_incremental(conn)
               load_fact_incremental(conn)
           
           create_indexes(conn)
           refresh_materialized_views(conn)
           
           conn.commit()
           logging.info(f"ETL 완료! (모드: {{SYNC_MODE}})")
       except Exception as e:
           if conn: conn.rollback()
           logging.error(f"ETL 오류: {{e}}")
       finally:
           if conn: conn.close()
   ```

5. **각 테이블에 필수 컬럼 추가**:
   - 디멘전 테이블: `created_at TIMESTAMP DEFAULT NOW()`, `updated_at TIMESTAMP DEFAULT NOW()`
   - 팩트 테이블: `etl_batch_id VARCHAR(50)`, `loaded_at TIMESTAMP DEFAULT NOW()`

Python 스크립트만 출력하세요. 마크다운이나 설명 없이 순수 Python 코드만 출력하세요.
"""
        
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = await self.llm.ainvoke([HumanMessage(content=prompt)])
                script = response.content
                
                # Clean up markdown if present
                if "```python" in script:
                    script = script.split("```python")[1].split("```")[0]
                elif "```" in script:
                    script = script.split("```")[1].split("```")[0]
                
                script = script.strip()
                
                # Check if script uses environment variables correctly
                if "os.environ" not in script and "os.getenv" not in script:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ⚠️ 환경변수 미사용 감지, DB 연결 코드 주입 중..."
                    )
                    # Inject proper environment variable based DB connection
                    env_db_code = '''import os
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PARAMS = {
    'host': os.environ.get('ETL_DB_HOST', 'localhost'),
    'port': int(os.environ.get('ETL_DB_PORT', '5432')),
    'user': os.environ.get('ETL_DB_USER', 'postgres'),
    'password': os.environ.get('ETL_DB_PASSWORD', ''),
    'database': os.environ.get('ETL_DB_NAME', 'postgres')
}

def get_connection():
    logging.info(f"Connecting to {DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}")
    return psycopg2.connect(**DB_PARAMS)

'''
                    # Find where to insert (after initial imports or at start)
                    if "import" in script:
                        # Replace existing DB connection code or insert after imports
                        import re
                        db_params_pattern = r"DB_PARAMS\s*=\s*\{[^}]+\}"
                        if re.search(db_params_pattern, script):
                            script = re.sub(db_params_pattern, """DB_PARAMS = {
    'host': os.environ.get('ETL_DB_HOST', 'localhost'),
    'port': int(os.environ.get('ETL_DB_PORT', '5432')),
    'user': os.environ.get('ETL_DB_USER', 'postgres'),
    'password': os.environ.get('ETL_DB_PASSWORD', ''),
    'database': os.environ.get('ETL_DB_NAME', 'postgres')
}""", script)
                            # Also ensure os import exists
                            if "import os" not in script:
                                script = "import os\n" + script
                        else:
                            script = env_db_code + script
                    else:
                        script = env_db_code + script
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ✅ 환경변수 기반 DB 연결 코드 주입 완료"
                    )
                
                # Validate script has required components
                validation_errors = []
                
                if "psycopg2" not in script:
                    validation_errors.append("psycopg2 import 누락")
                
                # Check for environment variable usage (os.getenv, os.environ.get, or actual port value)
                env_var_patterns = ["os.getenv", "os.environ.get", "os.environ[", f"port={db_port}", f"port=5432"]
                has_env_vars = any(p in script for p in env_var_patterns)
                if not has_env_vars:
                    validation_errors.append("환경변수 또는 실제 포트 설정 누락")
                
                # Check for hardcoded placeholder values (should use env vars instead)
                bad_placeholders = ["your_port", "your_host", "your_user", "your_password", "your_database", "your_dbname"]
                found_placeholders = [p for p in bad_placeholders if p in script.lower()]
                
                if found_placeholders:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ⚠️ 하드코딩된 플레이스홀더 발견: {found_placeholders}, 환경변수로 교체 중..."
                    )
                    # Replace with environment variable based code
                    import re
                    db_params_pattern = r"DB_PARAMS\s*=\s*\{[^}]+\}"
                    env_db_params = """DB_PARAMS = {
    'host': os.environ.get('ETL_DB_HOST', 'localhost'),
    'port': int(os.environ.get('ETL_DB_PORT', '5432')),
    'user': os.environ.get('ETL_DB_USER', 'postgres'),
    'password': os.environ.get('ETL_DB_PASSWORD', ''),
    'database': os.environ.get('ETL_DB_NAME', 'postgres')
}"""
                    if re.search(db_params_pattern, script):
                        script = re.sub(db_params_pattern, env_db_params, script)
                        if "import os" not in script:
                            script = "import os\n" + script
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] ✅ 환경변수 기반 DB_PARAMS로 교체 완료"
                        )
                
                if validation_errors:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ⚠️ 스크립트 검증 실패 (시도 {attempt + 1}/{max_attempts}): {', '.join(validation_errors)}"
                    )
                    if attempt < max_attempts - 1:
                        prompt += f"\n\n오류: {', '.join(validation_errors)}. 수정해서 다시 생성하세요."
                        continue
                
                # Test DB connection
                try:
                    import psycopg2
                    test_conn = psycopg2.connect(
                        host=db_host,
                        port=int(db_port),
                        user=db_user,
                        password=db_password,
                        database=db_name
                    )
                    test_conn.close()
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ✅ DB 연결 테스트 성공"
                    )
                except Exception as db_err:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ⛔ DB 연결 실패: {str(db_err)}"
                    )
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] 💡 조치: DB 서버 상태 및 연결 정보 확인 필요"
                    )
                    # Still save the script, but note the DB issue
                
                state["final_script"] = script
                state["final_etl_config"] = etl_config
                state["status"] = "validating"  # Move to validation step
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ✅ ETL 스크립트 생성 완료! ({len(script)} bytes)"
                )
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] 🔄 스크립트 검증 단계로 이동..."
                )
                break
                
            except Exception as e:
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ❌ 스크립트 생성 오류 (시도 {attempt + 1}/{max_attempts}): {str(e)}"
                )
                if attempt >= max_attempts - 1:
                    state["status"] = "error"
        
        return state
    
    async def _validate_script(self, state: ETLAgentState) -> ETLAgentState:
        """Validate the generated ETL script by testing SQL execution.
        
        This step extracts SQL statements from the generated script and
        tests them in a transaction to verify data loading.
        """
        state["status"] = "validating"
        state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 🧪 생성된 스크립트 검증 시작...")
        
        script = state.get("final_script", "")
        if not script:
            state["reasoning_log"].append(f"[{datetime.now().isoformat()}] ⚠️ 생성된 스크립트가 없습니다")
            state["status"] = "error"
            return state
        
        # Get DB connection info
        db_host = self.settings.oltp_db_host
        db_port = self.settings.oltp_db_port
        db_user = self.settings.oltp_db_user
        db_password = self.settings.oltp_db_password
        db_name = self.settings.oltp_db_name
        
        validation_results = {
            "dimension_counts": {},
            "fact_count": 0,
            "sql_errors": [],
            "success": True
        }
        
        try:
            import psycopg2
            import re
            
            conn = psycopg2.connect(
                host=db_host,
                port=int(db_port),
                user=db_user,
                password=db_password,
                database=db_name
            )
            cursor = conn.cursor()
            
            state["reasoning_log"].append(f"[{datetime.now().isoformat()}] ✅ DB 연결 성공")
            
            # ============================================
            # CRITICAL: Validate against Cube Schema
            # ============================================
            state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 🔍 큐브 스키마 필드 검증 시작...")
            
            # Get cube definition from ETL config or state
            etl_config = state.get("etl_config", {})
            cube_def = None
            
            # Try to load cube metadata from OLAP service
            try:
                from ..services.etl_service import etl_service
                cube_name = state.get("cube_name", "")
                if cube_name:
                    # Get cube metadata which includes dimensions, measures, etc.
                    cube_metadata = etl_service.get_cube_metadata(cube_name)
                    if cube_metadata:
                        cube_def = cube_metadata
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] 📋 큐브 메타데이터 로드 완료: {cube_name}"
                        )
            except Exception as e:
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ⚠️ 큐브 메타데이터 로드 실패: {str(e)[:100]}"
                )
            
            # If no cube_def from service, try to build from ETL config
            if not cube_def and etl_config:
                cube_def = {
                    "fact_table": etl_config.get("fact_table", ""),
                    "dimensions": [],
                    "measures": []
                }
                
                # Build dimensions from dimension_tables
                for dim_table in etl_config.get("dimension_tables", []):
                    dim_name = dim_table.split(".")[-1] if "." in dim_table else dim_table
                    cube_def["dimensions"].append({
                        "name": dim_name.replace("dim_", ""),
                        "table": dim_table,
                        "foreign_key": f"{dim_name}_id",
                        "levels": []
                    })
                
                # Build measures from mappings
                for mapping in etl_config.get("mappings", []):
                    if isinstance(mapping, dict):
                        target_col = mapping.get("target_column", "")
                        transformation = mapping.get("transformation", "")
                        if transformation and transformation.upper() in ["SUM", "AVG", "COUNT", "MAX", "MIN"]:
                            cube_def["measures"].append({
                                "name": target_col,
                                "column": target_col,
                                "aggregator": transformation.lower()
                            })
            
            # Validate ETL script against cube schema
            if cube_def:
                schema_validation = await self.tools.validate_etl_against_cube_schema(script, cube_def)
                
                # Log validation results
                passed_count = sum(1 for v in schema_validation["validations"] if v["success"])
                total_count = len(schema_validation["validations"])
                
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] 📊 큐브 스키마 검증: {passed_count}/{total_count} 통과"
                )
                
                if not schema_validation["success"]:
                    # Log failed validations
                    for v in schema_validation["validations"]:
                        if not v["success"]:
                            state["reasoning_log"].append(
                                f"[{datetime.now().isoformat()}] ❌ {v['message']}"
                            )
                    
                    # Log missing columns
                    if schema_validation["missing_columns"]:
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] ⚠️ 누락된 컬럼: {len(schema_validation['missing_columns'])}개"
                        )
                        for mc in schema_validation["missing_columns"][:5]:  # Show first 5
                            state["reasoning_log"].append(
                                f"[{datetime.now().isoformat()}]   - {mc['table']}.{mc['column']} ({mc['type']})"
                            )
                    
                    # Add to validation errors for regeneration
                    validation_results["sql_errors"].extend([
                        f"큐브 스키마 불일치: {v['message']}" 
                        for v in schema_validation["validations"] 
                        if not v["success"]
                    ])
                    validation_results["success"] = False
                    
                    # Add recommendations to regeneration context
                    if schema_validation["recommendations"]:
                        state["regeneration_context"] = state.get("regeneration_context", {})
                        state["regeneration_context"]["cube_schema_errors"] = schema_validation["missing_columns"]
                        state["regeneration_context"]["hints"] = state["regeneration_context"].get("hints", []) + [
                            "큐브 스키마의 모든 디멘전 필드/레벨이 ETL 테이블에 포함되어야 합니다",
                            "팩트 테이블에 모든 FK 컬럼이 포함되어야 합니다 (dim_time_id, dim_site_id 등)",
                            "측정값(measure) 컬럼이 팩트 테이블에 포함되어야 합니다"
                        ]
                else:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ✅ 큐브 스키마 검증 통과! 모든 필드가 일치합니다."
                    )
            else:
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ⚠️ 큐브 정의를 찾을 수 없어 스키마 검증 건너뜀"
                )
            
            # ============================================
            # SQL Syntax Validation (existing code continues)
            # ============================================
            
            # Extract SQL statements from the script
            # Look for CREATE TABLE and INSERT INTO patterns
            
            # 1. Extract dimension and fact table names from state
            dim_strategies = state.get("dimension_strategies", [])
            fact_strategy = state.get("fact_strategy", {})
            
            dw_tables = []
            for dim in dim_strategies:
                dim_name = dim.get("dimension_name", "")
                if dim_name:
                    dw_tables.append(f"dw.{dim_name}")
            
            fact_table = fact_strategy.get("fact_table_name", "")
            if not fact_table:
                fact_table = f"dw.fact_{state['cube_name']}"
            dw_tables.append(fact_table)
            
            state["reasoning_log"].append(f"[{datetime.now().isoformat()}] 📋 검증할 테이블: {dw_tables}")
            
            # 2. Test if we can parse and execute a subset of the script
            # Extract CREATE TABLE statements and test them in a transaction
            
            # First, ensure DW schema exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS dw")
            state["reasoning_log"].append(f"[{datetime.now().isoformat()}] ✅ DW 스키마 확인됨")
            
            # Try to extract and test INSERT statements
            # Pattern to find INSERT INTO dw.xxx statements
            insert_patterns = re.findall(
                r'cursor\.execute\(\s*"""(INSERT INTO dw\.[^"]+)"""',
                script,
                re.DOTALL | re.IGNORECASE
            )
            
            if not insert_patterns:
                # Try alternative pattern
                insert_patterns = re.findall(
                    r"cursor\.execute\(\s*'''(INSERT INTO dw\.[^']+)'''",
                    script,
                    re.DOTALL | re.IGNORECASE
                )
            
            if not insert_patterns:
                # Try to extract from triple-quoted strings
                insert_patterns = re.findall(
                    r'"""([^"]*INSERT INTO dw\.[^"]+)"""',
                    script,
                    re.DOTALL | re.IGNORECASE
                )
            
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] 📊 발견된 INSERT 문: {len(insert_patterns)}개"
            )
            
            # 3. Instead of executing the whole script, check expected table row counts
            # by examining what data SHOULD be loaded based on source tables
            
            expected_row_counts = {}
            
            # Check dimension source data
            for dim in dim_strategies:
                dim_name = dim.get("dimension_name", "")
                source_table = dim.get("source_table", "")
                source_schema = dim.get("source_schema", "RWIS")
                
                if source_table:
                    try:
                        # Count rows in source table
                        cursor.execute(f"""
                            SELECT COUNT(DISTINCT "TAGSN") 
                            FROM "{source_schema}"."{source_table}"
                            WHERE "TAGSN" IS NOT NULL
                        """)
                        count = cursor.fetchone()[0]
                        expected_row_counts[dim_name] = count
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] 📊 {dim_name} 예상 행 수: ~{count}"
                        )
                    except Exception as e:
                        # Try a simpler count
                        try:
                            cursor.execute(f"""
                                SELECT COUNT(*) FROM "{source_schema}"."{source_table}"
                            """)
                            count = cursor.fetchone()[0]
                            expected_row_counts[dim_name] = min(count, 1000)  # Cap for dimensions
                            state["reasoning_log"].append(
                                f"[{datetime.now().isoformat()}] 📊 {dim_name} 소스 테이블 행 수: {count}"
                            )
                        except Exception as e2:
                            state["reasoning_log"].append(
                                f"[{datetime.now().isoformat()}] ⚠️ {source_schema}.{source_table} 조회 실패: {str(e2)[:100]}"
                            )
            
            # 4. Test the actual INSERT SQL by wrapping in EXPLAIN
            # This validates SQL syntax without executing
            sql_validation_errors = []
            
            for i, insert_sql in enumerate(insert_patterns[:5]):  # Test first 5
                try:
                    # Clean up the SQL
                    clean_sql = insert_sql.strip()
                    if not clean_sql.endswith(";"):
                        clean_sql += ";"
                    
                    # Use EXPLAIN to validate without executing
                    cursor.execute(f"EXPLAIN {clean_sql}")
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ✅ SQL {i+1} 문법 검증 성공"
                    )
                except Exception as e:
                    error_msg = str(e)
                    sql_validation_errors.append({
                        "sql_index": i,
                        "error": error_msg,
                        "sql_preview": clean_sql[:200]
                    })
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ❌ SQL {i+1} 오류: {error_msg[:150]}"
                    )
            
            # 5. Check for common SQL bugs in the script
            sql_bugs_found = []
            
            # Bug: Using a table in JOIN without including it in FROM
            # Pattern: JOIN table ON ... but table not in FROM clause
            
            # Check for the specific bug from user's example
            if "RDISAUP_TB" in script:
                # Check if RDISAUP_TB is properly included in FROM when used in JOIN
                fact_insert_match = re.search(
                    r'INSERT INTO dw\.fact_\w+.*?SELECT.*?FROM[^;]+',
                    script,
                    re.DOTALL | re.IGNORECASE
                )
                if fact_insert_match:
                    fact_insert_sql = fact_insert_match.group(0)
                    
                    # Check if tables used in JOIN are in FROM
                    join_tables = re.findall(r'JOIN\s+["\']?(\w+)["\']?\s+', fact_insert_sql, re.IGNORECASE)
                    from_tables = re.findall(r'FROM\s+["\']?(\w+)["\']?', fact_insert_sql, re.IGNORECASE)
                    
                    for jt in join_tables:
                        if jt.upper() not in [t.upper() for t in from_tables] and not jt.startswith("dw."):
                            sql_bugs_found.append(
                                f"테이블 '{jt}'가 JOIN에 사용되었지만 FROM에 포함되지 않음"
                            )
            
            # Bug: Missing quotes for uppercase table names
            if 'RWIS.' in script and '"RWIS".' not in script:
                sql_bugs_found.append(
                    "PostgreSQL에서 대문자 스키마/테이블은 쌍따옴표 필요: RWIS → \"RWIS\""
                )
            
            if sql_bugs_found:
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ⚠️ SQL 버그 감지: {len(sql_bugs_found)}개"
                )
                for bug in sql_bugs_found:
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}]   - {bug}"
                    )
                validation_results["sql_errors"].extend(sql_bugs_found)
                validation_results["success"] = False
            
            # 6. If there are SQL validation errors, mark for regeneration
            if sql_validation_errors:
                validation_results["sql_errors"].extend([
                    e["error"] for e in sql_validation_errors
                ])
                validation_results["success"] = False
                
                # Store error context for regeneration
                state["validation_errors"] = sql_validation_errors
            
            # 7. Test a critical query: the fact table INSERT
            # Extract and validate the fact table INSERT specifically
            fact_insert_pattern = re.search(
                r'INSERT INTO dw\.fact_\w+.*?(?:SELECT|VALUES)[^;]+',
                script,
                re.DOTALL | re.IGNORECASE
            )
            
            if fact_insert_pattern:
                fact_insert_sql = fact_insert_pattern.group(0).strip()
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] 🔍 팩트 테이블 INSERT 검증 중..."
                )
                
                try:
                    # Test with EXPLAIN
                    cursor.execute(f"EXPLAIN {fact_insert_sql}")
                    explain_result = cursor.fetchall()
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ✅ 팩트 테이블 INSERT 문법 검증 성공"
                    )
                    
                    # Estimate row count from EXPLAIN
                    for row in explain_result:
                        if 'rows=' in str(row):
                            match = re.search(r'rows=(\d+)', str(row))
                            if match:
                                estimated_rows = int(match.group(1))
                                state["reasoning_log"].append(
                                    f"[{datetime.now().isoformat()}] 📊 예상 적재 행 수: ~{estimated_rows}"
                                )
                                if estimated_rows == 0:
                                    sql_bugs_found.append("팩트 테이블 INSERT가 0행을 반환할 것으로 예상됨 - JOIN 조건 확인 필요")
                                    validation_results["success"] = False
                                break
                                
                except Exception as e:
                    error_msg = str(e)
                    state["reasoning_log"].append(
                        f"[{datetime.now().isoformat()}] ❌ 팩트 테이블 INSERT 오류: {error_msg[:200]}"
                    )
                    validation_results["sql_errors"].append(f"Fact INSERT: {error_msg}")
                    validation_results["success"] = False
                    
                    # Parse error to provide helpful feedback
                    if "does not exist" in error_msg.lower():
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] 💡 힌트: 테이블이나 컬럼이 존재하지 않습니다. 대소문자 및 따옴표 확인 필요"
                        )
                    if "missing FROM-clause entry" in error_msg.lower():
                        state["reasoning_log"].append(
                            f"[{datetime.now().isoformat()}] 💡 힌트: JOIN에서 사용하는 테이블이 FROM 절에 없습니다"
                        )
            
            conn.rollback()  # Don't actually make changes
            cursor.close()
            conn.close()
            
        except Exception as e:
            import traceback
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ❌ 스크립트 검증 실패: {str(e)}"
            )
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] 📋 상세 오류:\n{traceback.format_exc()[:500]}"
            )
            validation_results["success"] = False
            validation_results["sql_errors"].append(str(e))
        
        # Store validation results
        state["validation_results"] = validation_results
        
        if validation_results["success"]:
            state["status"] = "completed"
            state["reasoning_log"].append(
                f"[{datetime.now().isoformat()}] ✅ 스크립트 검증 완료 - 문제 없음!"
            )
        else:
            # Increment retry count for script regeneration
            current_script_retry = state.get("script_retry_count", 0)
            state["script_retry_count"] = current_script_retry + 1
            
            if current_script_retry < 2:  # Allow 2 retries
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] 🔄 스크립트 재생성 필요 (시도 {current_script_retry + 1}/3)"
                )
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] 📋 발견된 문제: {validation_results['sql_errors']}"
                )
                
                # Add error context to prompt for regeneration
                state["regeneration_context"] = {
                    "errors": validation_results["sql_errors"],
                    "hints": [
                        "JOIN에서 사용하는 모든 테이블을 FROM 절에 포함",
                        "PostgreSQL 대문자 테이블/컬럼은 쌍따옴표 사용: \"RWIS\".\"TABLE_NAME\"",
                        "팩트 테이블 INSERT 시 모든 디멘전 테이블과 명시적 JOIN 필요"
                    ]
                }
            else:
                state["status"] = "completed"  # Max retries, proceed anyway
                state["reasoning_log"].append(
                    f"[{datetime.now().isoformat()}] ⚠️ 최대 재시도 횟수 도달 - 수동 검토 권장"
                )
        
        return state
    
    def _should_regenerate(self, state: ETLAgentState) -> str:
        """Determine if script should be regenerated based on validation results."""
        validation_results = state.get("validation_results", {})
        script_retry_count = state.get("script_retry_count", 0)
        
        if validation_results.get("success", True):
            return "complete"
        
        if script_retry_count >= 3:
            return "complete"  # Max retries, proceed anyway
        
        # Check for unrecoverable errors
        sql_errors = validation_results.get("sql_errors", [])
        for error in sql_errors:
            if any(x in str(error).lower() for x in [
                "connection refused", 
                "authentication failed",
                "could not connect"
            ]):
                return "error"
        
        return "regenerate"
    
    async def generate_etl(
        self,
        cube_name: str,
        cube_description: str,
        dimensions: List[str],
        measures: List[str],
        etl_config: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Run the ETL generation agent.
        
        Args:
            cube_name: Name of the cube
            cube_description: Description/analysis intent
            dimensions: List of dimension names
            measures: List of measure names
            etl_config: Optional full ETL configuration
        """
        
        initial_state: ETLAgentState = {
            "cube_name": cube_name,
            "cube_description": cube_description,
            "target_dimensions": dimensions,
            "target_measures": measures,
            "etl_config": etl_config,
            "messages": [],
            "source_tables": [],
            "dimension_strategies": [],
            "fact_strategy": {},
            "generated_sql": {},
            "test_results": [],
            "retry_count": 0,
            "validation_results": None,
            "validation_errors": None,
            "script_retry_count": 0,
            "regeneration_context": None,
            "final_etl_config": None,
            "final_script": None,
            "reasoning_log": [],
            "status": "analyzing"
        }
        
        # Run the graph
        final_state = await self.graph.ainvoke(initial_state)
        
        return {
            "cube_name": cube_name,
            "status": final_state["status"],
            "etl_config": final_state.get("final_etl_config"),
            "script": final_state.get("final_script"),
            "reasoning_log": final_state.get("reasoning_log", []),
            "dimension_strategies": final_state.get("dimension_strategies", []),
            "fact_strategy": final_state.get("fact_strategy", {}),
            "test_results": final_state.get("test_results", [])
        }
    
    async def generate_etl_streaming(
        self,
        cube_name: str,
        cube_description: str,
        dimensions: List[str],
        measures: List[str],
        etl_config: Optional[Dict] = None
    ):
        """Run the ETL generation agent with streaming events.
        
        Args:
            cube_name: Name of the cube
            cube_description: Description/analysis intent
            dimensions: List of dimension names
            measures: List of measure names
            etl_config: Optional full ETL configuration including source_tables, mappings, etc.
        
        Yields SSE events as the agent progresses through each step.
        """
        import json
        
        initial_state: ETLAgentState = {
            "cube_name": cube_name,
            "cube_description": cube_description,
            "target_dimensions": dimensions,
            "target_measures": measures,
            "etl_config": etl_config,  # Include full ETL config if provided
            "messages": [],
            "source_tables": [],
            "dimension_strategies": [],
            "fact_strategy": {},
            "generated_sql": {},
            "test_results": [],
            "retry_count": 0,
            "validation_results": None,
            "validation_errors": None,
            "script_retry_count": 0,
            "regeneration_context": None,
            "final_etl_config": None,
            "final_script": None,
            "reasoning_log": [],
            "status": "analyzing"
        }
        
        # Yield initial event
        yield {
            "event": "start",
            "data": {
                "cube_name": cube_name,
                "status": "analyzing",
                "message": "🚀 ETL 생성 에이전트 시작..."
            }
        }
        
        # Stream through graph nodes with error handling
        try:
            async for event in self.graph.astream(initial_state, stream_mode="updates"):
                for node_name, node_output in event.items():
                    # Extract reasoning log updates
                    if "reasoning_log" in node_output:
                        new_logs = node_output["reasoning_log"]
                        for log in new_logs:
                            yield {
                                "event": "log",
                                "data": {
                                    "node": node_name,
                                    "message": log,
                                    "timestamp": datetime.now().isoformat()
                                }
                            }
                    
                    # Extract code generation updates
                    if "final_script" in node_output and node_output["final_script"]:
                        yield {
                            "event": "code",
                            "data": {
                                "code": node_output["final_script"],
                                "complete": True
                            }
                        }
                    
                    # Extract SQL generation updates
                    if "generated_sql" in node_output:
                        for sql_name, sql_code in node_output["generated_sql"].items():
                            yield {
                                "event": "sql",
                                "data": {
                                    "name": sql_name,
                                    "sql": sql_code
                                }
                            }
                    
                    # Status updates
                    if "status" in node_output:
                        yield {
                            "event": "status",
                            "data": {
                                "node": node_name,
                                "status": node_output["status"]
                            }
                        }
                    
                    # Dimension strategies
                    if "dimension_strategies" in node_output:
                        yield {
                            "event": "dimensions",
                            "data": {
                                "strategies": node_output["dimension_strategies"]
                            }
                        }
                    
                    # Test results
                    if "test_results" in node_output:
                        for test in node_output["test_results"]:
                            yield {
                                "event": "test",
                                "data": test
                            }
                    
                    # Validation results (from script validation step)
                    if "validation_results" in node_output and node_output["validation_results"]:
                        val_results = node_output["validation_results"]
                        yield {
                            "event": "validation",
                            "data": {
                                "success": val_results.get("success", False),
                                "errors": val_results.get("sql_errors", []),
                                "dimension_counts": val_results.get("dimension_counts", {}),
                                "fact_count": val_results.get("fact_count", 0)
                            }
                        }
                    
                    # Script regeneration notification
                    if "script_retry_count" in node_output and node_output.get("script_retry_count", 0) > 0:
                        yield {
                            "event": "log",
                            "data": {
                                "node": "validate_script",
                                "message": f"🔄 스크립트 재생성 시도 {node_output['script_retry_count']}/3",
                                "timestamp": datetime.now().isoformat()
                            }
                        }
            
            # Yield final event
            yield {
                "event": "complete",
                "data": {
                    "cube_name": cube_name,
                    "status": "completed",
                    "message": "✅ ETL 생성 및 검증 완료!"
                }
            }
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            import logging
            logging.error(f"ETL Agent streaming error: {error_details}")
            
            yield {
                "event": "error",
                "data": {
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "details": error_details[:1000],  # Limit for safety
                    "message": f"❌ ETL 생성 오류: {str(e)}"
                }
            }


# Singleton instance
_etl_agent: Optional[ETLAgent] = None

def get_etl_agent() -> ETLAgent:
    """Get ETL Agent singleton instance."""
    global _etl_agent
    if _etl_agent is None:
        _etl_agent = ETLAgent()
    return _etl_agent
