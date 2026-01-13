"""Direct ETL Service - Execute ETL pipelines directly using Python.

This service handles:
1. Direct execution of ETL without Airflow
2. Schema and table creation
3. Data synchronization from source to DW
"""
import os
import psycopg2
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from ..core.config import get_settings


@dataclass
class ETLExecutionResult:
    """Result of an ETL execution."""
    success: bool
    cube_name: str
    started_at: str
    completed_at: str
    steps: List[Dict[str, Any]]
    error: Optional[str] = None
    total_rows_processed: int = 0


class DirectETLService:
    """Service for executing ETL pipelines directly without Airflow."""
    
    def __init__(self):
        self.settings = get_settings()
    
    def _get_db_connection(self):
        """Get database connection using settings."""
        return psycopg2.connect(
            host=os.getenv('OLTP_DB_HOST', 'localhost'),
            port=os.getenv('OLTP_DB_PORT', '5432'),
            user=os.getenv('OLTP_DB_USER', 'postgres'),
            password=os.getenv('OLTP_DB_PASSWORD', 'postgres123'),
            database=os.getenv('OLTP_DB_NAME', 'meetingroom')
        )
    
    def execute_etl(self, etl_config: Dict) -> ETLExecutionResult:
        """Execute ETL pipeline directly.
        
        Args:
            etl_config: ETL configuration dictionary containing:
                - cube_name: Name of the cube
                - fact_table: Target fact table name
                - dimension_tables: List of dimension table names
                - source_tables: List of source table names
                - mappings: Column mappings from source to target
                - dw_schema: Data warehouse schema name
                - sync_mode: 'full' or 'incremental'
                - incremental_column: Column for incremental sync
        
        Returns:
            ETLExecutionResult with execution details
        """
        cube_name = etl_config.get("cube_name", "unnamed")
        fact_table = etl_config.get("fact_table", "")
        dimension_tables = etl_config.get("dimension_tables", [])
        mappings = etl_config.get("mappings", [])
        dw_schema = etl_config.get("dw_schema", "dw")
        sync_mode = etl_config.get("sync_mode", "full")
        incremental_column = etl_config.get("incremental_column")
        
        started_at = datetime.now().isoformat()
        steps = []
        total_rows = 0
        
        try:
            # Step 1: Create DW Schema
            step_result = self._create_dw_schema(dw_schema)
            steps.append(step_result)
            
            # Step 2: Create Dimension Tables
            step_result = self._create_dimension_tables(dw_schema, dimension_tables, mappings)
            steps.append(step_result)
            
            # Step 3: Create Fact Table
            step_result = self._create_fact_table(dw_schema, fact_table, dimension_tables, mappings)
            steps.append(step_result)
            
            # Step 4: Sync Dimension Tables
            for dim_table in dimension_tables:
                step_result = self._sync_dimension(dw_schema, dim_table, mappings)
                steps.append(step_result)
                total_rows += step_result.get("rows_processed", 0)
            
            # Step 5: Sync Fact Table
            step_result = self._sync_fact_table(dw_schema, fact_table, dimension_tables, mappings, sync_mode, incremental_column)
            steps.append(step_result)
            total_rows += step_result.get("rows_processed", 0)
            
            return ETLExecutionResult(
                success=True,
                cube_name=cube_name,
                started_at=started_at,
                completed_at=datetime.now().isoformat(),
                steps=steps,
                total_rows_processed=total_rows
            )
            
        except Exception as e:
            return ETLExecutionResult(
                success=False,
                cube_name=cube_name,
                started_at=started_at,
                completed_at=datetime.now().isoformat(),
                steps=steps,
                error=str(e),
                total_rows_processed=total_rows
            )
    
    def _create_dw_schema(self, dw_schema: str) -> Dict[str, Any]:
        """Create DW schema if not exists."""
        conn = self._get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {dw_schema}")
            conn.commit()
            return {
                "step": "create_dw_schema",
                "status": "success",
                "message": f"Schema {dw_schema} created/verified"
            }
        except Exception as e:
            conn.rollback()
            return {
                "step": "create_dw_schema",
                "status": "error",
                "message": str(e)
            }
        finally:
            cur.close()
            conn.close()
    
    def _create_dimension_tables(self, dw_schema: str, dimension_tables: List[str], mappings: List[Dict]) -> Dict[str, Any]:
        """Create dimension tables if not exist."""
        conn = self._get_db_connection()
        cur = conn.cursor()
        created_tables = []
        
        try:
            for dim_table in dimension_tables:
                dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
                full_table = f"{dw_schema}.{dim_name}"
                
                # Find columns for this dimension from mappings
                dim_mappings = [m for m in mappings if m.get('target_table') == dim_name]
                
                columns = ["id SERIAL PRIMARY KEY"]
                for m in dim_mappings:
                    col_name = m.get('target_column', '')
                    if col_name and col_name != 'id':
                        columns.append(f"{col_name} VARCHAR(255)")
                
                # Add common dimension columns if no mappings found
                if len(columns) == 1:
                    columns.append("name VARCHAR(255)")
                    columns.append("code VARCHAR(100)")
                
                columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
                
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {full_table} (
                        {', '.join(columns)}
                    )
                """
                cur.execute(create_sql)
                created_tables.append(full_table)
            
            conn.commit()
            return {
                "step": "create_dimension_tables",
                "status": "success",
                "message": f"Created/verified {len(created_tables)} dimension tables",
                "tables": created_tables
            }
        except Exception as e:
            conn.rollback()
            return {
                "step": "create_dimension_tables",
                "status": "error",
                "message": str(e)
            }
        finally:
            cur.close()
            conn.close()
    
    def _create_fact_table(self, dw_schema: str, fact_table: str, dimension_tables: List[str], mappings: List[Dict]) -> Dict[str, Any]:
        """Create fact table if not exists."""
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        try:
            fact_name = fact_table.split('.')[-1] if '.' in fact_table else fact_table
            full_table = f"{dw_schema}.{fact_name}"
            
            # Find columns for fact table from mappings
            fact_mappings = [m for m in mappings if m.get('target_table') == fact_name]
            
            columns = ["id SERIAL PRIMARY KEY"]
            
            # Add FK columns for each dimension
            for dim_table in dimension_tables:
                dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
                columns.append(f"{dim_name}_id INTEGER")
            
            # Add measure columns
            for m in fact_mappings:
                col_name = m.get('target_column', '')
                if col_name and col_name != 'id':
                    columns.append(f"{col_name} NUMERIC(20,4)")
            
            columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                    {', '.join(columns)}
                )
            """
            cur.execute(create_sql)
            conn.commit()
            
            return {
                "step": "create_fact_table",
                "status": "success",
                "message": f"Created/verified fact table {full_table}"
            }
        except Exception as e:
            conn.rollback()
            return {
                "step": "create_fact_table",
                "status": "error",
                "message": str(e)
            }
        finally:
            cur.close()
            conn.close()
    
    def _sync_dimension(self, dw_schema: str, dim_table: str, mappings: List[Dict]) -> Dict[str, Any]:
        """Sync a dimension table from source."""
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        try:
            dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
            dim_mappings = [m for m in mappings if m.get('target_table') == dim_name]
            
            if not dim_mappings:
                return {
                    "step": f"sync_dimension_{dim_name}",
                    "status": "skipped",
                    "message": f"No mappings found for {dim_table}",
                    "rows_processed": 0
                }
            
            # Build columns
            source_cols = []
            target_cols = []
            for m in dim_mappings:
                source_expr = m.get('transformation') or f"{m['source_table']}.{m['source_column']}"
                source_cols.append(f"{source_expr} AS {m['target_column']}")
                target_cols.append(m['target_column'])
            
            # Get unique source tables
            source_tables = list(set([m['source_table'] for m in dim_mappings]))
            from_clause = ", ".join(source_tables)
            
            full_table = f"{dw_schema}.{dim_name}"
            
            # Generate INSERT query
            insert_sql = f"""
                INSERT INTO {full_table} ({', '.join(target_cols)})
                SELECT DISTINCT {', '.join(source_cols)}
                FROM {from_clause}
                ON CONFLICT DO NOTHING
            """
            
            cur.execute(insert_sql)
            rows = cur.rowcount
            conn.commit()
            
            return {
                "step": f"sync_dimension_{dim_name}",
                "status": "success",
                "message": f"Inserted {rows} rows into {full_table}",
                "rows_processed": rows
            }
            
        except Exception as e:
            conn.rollback()
            return {
                "step": f"sync_dimension_{dim_table}",
                "status": "error",
                "message": str(e),
                "rows_processed": 0
            }
        finally:
            cur.close()
            conn.close()
    
    def _sync_fact_table(self, dw_schema: str, fact_table: str, dimension_tables: List[str], 
                         mappings: List[Dict], sync_mode: str, incremental_column: Optional[str]) -> Dict[str, Any]:
        """Sync fact table from source."""
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        try:
            fact_name = fact_table.split('.')[-1] if '.' in fact_table else fact_table
            fact_mappings = [m for m in mappings if m.get('target_table') == fact_name]
            
            if not fact_mappings:
                return {
                    "step": "sync_fact_table",
                    "status": "skipped",
                    "message": f"No mappings found for {fact_table}",
                    "rows_processed": 0
                }
            
            # Build columns
            source_cols = []
            target_cols = []
            for m in fact_mappings:
                source_expr = m.get('transformation') or f"{m['source_table']}.{m['source_column']}"
                source_cols.append(f"{source_expr} AS {m['target_column']}")
                target_cols.append(m['target_column'])
            
            # Get unique source tables
            source_tables = list(set([m['source_table'] for m in fact_mappings]))
            from_clause = ", ".join(source_tables)
            
            full_table = f"{dw_schema}.{fact_name}"
            
            # Build WHERE clause for incremental
            where_clause = ""
            if sync_mode == "incremental" and incremental_column:
                where_clause = f"WHERE {incremental_column} > (SELECT COALESCE(MAX(_etl_loaded_at), '1900-01-01') FROM {full_table})"
            
            # Generate INSERT query
            insert_sql = f"""
                INSERT INTO {full_table} ({', '.join(target_cols)})
                SELECT {', '.join(source_cols)}
                FROM {from_clause}
                {where_clause}
            """
            
            cur.execute(insert_sql)
            rows = cur.rowcount
            conn.commit()
            
            return {
                "step": "sync_fact_table",
                "status": "success",
                "message": f"Inserted {rows} rows into {full_table}",
                "rows_processed": rows
            }
            
        except Exception as e:
            conn.rollback()
            return {
                "step": "sync_fact_table",
                "status": "error",
                "message": str(e),
                "rows_processed": 0
            }
        finally:
            cur.close()
            conn.close()
    
    def generate_python_code(self, etl_config: Dict) -> str:
        """Generate standalone Python ETL script code."""
        
        cube_name = etl_config.get("cube_name", "unnamed")
        fact_table = etl_config.get("fact_table", "")
        dimension_tables = etl_config.get("dimension_tables", [])
        mappings = etl_config.get("mappings", [])
        dw_schema = etl_config.get("dw_schema", "dw")
        sync_mode = etl_config.get("sync_mode", "full")
        incremental_column = etl_config.get("incremental_column")
        
        import json
        
        code = f'''#!/usr/bin/env python3
"""
ETL Script for: {cube_name}
Generated: {datetime.now().isoformat()}

This script performs ETL from source tables to the data warehouse.
Run directly with: python etl_{cube_name}.py
"""
import os
import psycopg2
from datetime import datetime

# Configuration
CUBE_NAME = "{cube_name}"
FACT_TABLE = "{fact_table}"
DW_SCHEMA = "{dw_schema}"
SYNC_MODE = "{sync_mode}"
INCREMENTAL_COLUMN = {repr(incremental_column)}

DIMENSION_TABLES = {json.dumps(dimension_tables, ensure_ascii=False, indent=4)}

MAPPINGS = {json.dumps(mappings, ensure_ascii=False, indent=4)}


def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres123'),
        database=os.getenv('DB_NAME', 'meetingroom')
    )


def create_schema():
    """Create DW schema."""
    print(f"Creating schema {{DW_SCHEMA}}...")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {{DW_SCHEMA}}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"Schema {{DW_SCHEMA}} ready.")


def create_dimension_tables():
    """Create dimension tables."""
    print("Creating dimension tables...")
    conn = get_connection()
    cur = conn.cursor()
    
    for dim_table in DIMENSION_TABLES:
        dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
        full_table = f"{{DW_SCHEMA}}.{{dim_name}}"
        
        dim_mappings = [m for m in MAPPINGS if m.get('target_table') == dim_name]
        
        columns = ["id SERIAL PRIMARY KEY"]
        for m in dim_mappings:
            col = m.get('target_column', '')
            if col and col != 'id':
                columns.append(f"{{col}} VARCHAR(255)")
        
        if len(columns) == 1:
            columns.extend(["name VARCHAR(255)", "code VARCHAR(100)"])
        
        columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
        
        sql = f"CREATE TABLE IF NOT EXISTS {{full_table}} ({{', '.join(columns)}})"
        cur.execute(sql)
        print(f"  ✓ {{full_table}}")
    
    conn.commit()
    cur.close()
    conn.close()


def create_fact_table():
    """Create fact table."""
    print(f"Creating fact table {{FACT_TABLE}}...")
    conn = get_connection()
    cur = conn.cursor()
    
    fact_name = FACT_TABLE.split('.')[-1] if '.' in FACT_TABLE else FACT_TABLE
    full_table = f"{{DW_SCHEMA}}.{{fact_name}}"
    
    fact_mappings = [m for m in MAPPINGS if m.get('target_table') == fact_name]
    
    columns = ["id SERIAL PRIMARY KEY"]
    
    for dim_table in DIMENSION_TABLES:
        dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
        columns.append(f"{{dim_name}}_id INTEGER")
    
    for m in fact_mappings:
        col = m.get('target_column', '')
        if col and col != 'id':
            columns.append(f"{{col}} NUMERIC(20,4)")
    
    columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
    
    sql = f"CREATE TABLE IF NOT EXISTS {{full_table}} ({{', '.join(columns)}})"
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print(f"  ✓ {{full_table}}")


def sync_dimensions():
    """Sync dimension tables."""
    print("Syncing dimension tables...")
    conn = get_connection()
    cur = conn.cursor()
    
    for dim_table in DIMENSION_TABLES:
        dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
        dim_mappings = [m for m in MAPPINGS if m.get('target_table') == dim_name]
        
        if not dim_mappings:
            print(f"  ⊘ {{dim_name}} - no mappings")
            continue
        
        source_cols = []
        target_cols = []
        for m in dim_mappings:
            expr = m.get('transformation') or f"{{m['source_table']}}.{{m['source_column']}}"
            source_cols.append(f"{{expr}} AS {{m['target_column']}}")
            target_cols.append(m['target_column'])
        
        source_tables = list(set([m['source_table'] for m in dim_mappings]))
        full_table = f"{{DW_SCHEMA}}.{{dim_name}}"
        
        sql = f\"\"\"
            INSERT INTO {{full_table}} ({{', '.join(target_cols)}})
            SELECT DISTINCT {{', '.join(source_cols)}}
            FROM {{', '.join(source_tables)}}
            ON CONFLICT DO NOTHING
        \"\"\"
        
        cur.execute(sql)
        rows = cur.rowcount
        print(f"  ✓ {{dim_name}}: {{rows}} rows")
    
    conn.commit()
    cur.close()
    conn.close()


def sync_fact_table():
    """Sync fact table."""
    print(f"Syncing fact table {{FACT_TABLE}}...")
    conn = get_connection()
    cur = conn.cursor()
    
    fact_name = FACT_TABLE.split('.')[-1] if '.' in FACT_TABLE else FACT_TABLE
    fact_mappings = [m for m in MAPPINGS if m.get('target_table') == fact_name]
    
    if not fact_mappings:
        print(f"  ⊘ No mappings for fact table")
        return
    
    source_cols = []
    target_cols = []
    for m in fact_mappings:
        expr = m.get('transformation') or f"{{m['source_table']}}.{{m['source_column']}}"
        source_cols.append(f"{{expr}} AS {{m['target_column']}}")
        target_cols.append(m['target_column'])
    
    source_tables = list(set([m['source_table'] for m in fact_mappings]))
    full_table = f"{{DW_SCHEMA}}.{{fact_name}}"
    
    where = ""
    if SYNC_MODE == "incremental" and INCREMENTAL_COLUMN:
        where = f"WHERE {{INCREMENTAL_COLUMN}} > (SELECT COALESCE(MAX(_etl_loaded_at), '1900-01-01') FROM {{full_table}})"
    
    sql = f\"\"\"
        INSERT INTO {{full_table}} ({{', '.join(target_cols)}})
        SELECT {{', '.join(source_cols)}}
        FROM {{', '.join(source_tables)}}
        {{where}}
    \"\"\"
    
    cur.execute(sql)
    rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"  ✓ {{fact_name}}: {{rows}} rows")


def main():
    """Run ETL pipeline."""
    print(f"=" * 60)
    print(f"ETL Pipeline: {{CUBE_NAME}}")
    print(f"Started: {{datetime.now().isoformat()}}")
    print(f"=" * 60)
    
    try:
        create_schema()
        create_dimension_tables()
        create_fact_table()
        sync_dimensions()
        sync_fact_table()
        
        print(f"\\n✅ ETL completed successfully!")
        print(f"Finished: {{datetime.now().isoformat()}}")
    except Exception as e:
        print(f"\\n❌ ETL failed: {{e}}")
        raise


if __name__ == "__main__":
    main()
'''
        return code


# Singleton instance
_direct_etl_service: Optional[DirectETLService] = None

def get_direct_etl_service() -> DirectETLService:
    """Get DirectETLService singleton instance."""
    global _direct_etl_service
    if _direct_etl_service is None:
        _direct_etl_service = DirectETLService()
    return _direct_etl_service
