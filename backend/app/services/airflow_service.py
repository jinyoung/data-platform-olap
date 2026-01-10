"""Airflow Service - Generate and manage Airflow DAGs for ETL pipelines.

This service handles:
1. Converting ETL configs to Airflow DAG Python code
2. Saving DAGs to the Airflow dags folder
3. Triggering DAG runs via Airflow REST API
4. Monitoring DAG status
"""
import os
import json
import httpx
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from ..core.config import get_settings


# Airflow configuration
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

# DAGs folder path (mounted volume)
DAGS_FOLDER = Path(__file__).parent.parent.parent.parent / "airflow" / "dags"


@dataclass
class DAGInfo:
    """Information about a generated DAG."""
    dag_id: str
    file_path: str
    cube_name: str
    created_at: str
    airflow_url: str


class AirflowService:
    """Service for managing Airflow ETL DAGs."""
    
    def __init__(self):
        self.settings = get_settings()
        self.airflow_host = AIRFLOW_HOST
        self.auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)
    
    def generate_dag_code(self, etl_config: Dict) -> str:
        """Generate Airflow DAG Python code from ETL config."""
        
        cube_name = etl_config.get("cube_name", "unnamed")
        dag_id = self._sanitize_dag_id(cube_name)
        fact_table = etl_config.get("fact_table", "")
        dimension_tables = etl_config.get("dimension_tables", [])
        source_tables = etl_config.get("source_tables", [])
        mappings = etl_config.get("mappings", [])
        dw_schema = etl_config.get("dw_schema", "dw")
        sync_mode = etl_config.get("sync_mode", "full")
        incremental_column = etl_config.get("incremental_column")
        
        # Generate mapping code
        mappings_json = json.dumps(mappings, ensure_ascii=False, indent=8)
        
        dag_code = f'''"""
Auto-generated Airflow DAG for ETL Pipeline
Cube: {cube_name}
Generated: {datetime.now().isoformat()}
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import os

# DAG Configuration
DAG_ID = "{dag_id}"
CUBE_NAME = "{cube_name}"
FACT_TABLE = "{fact_table}"
DW_SCHEMA = "{dw_schema}"
SYNC_MODE = "{sync_mode}"
INCREMENTAL_COLUMN = {repr(incremental_column)}

DIMENSION_TABLES = {json.dumps(dimension_tables, ensure_ascii=False)}
SOURCE_TABLES = {json.dumps(source_tables, ensure_ascii=False)}
MAPPINGS = {mappings_json}

# Default arguments
default_args = {{
    'owner': 'olap-etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}


def get_db_connection():
    """Get database connection using environment variables."""
    import psycopg2
    return psycopg2.connect(
        host=os.getenv('OLTP_DB_HOST', 'host.docker.internal'),
        port=os.getenv('OLTP_DB_PORT', '5432'),
        user=os.getenv('OLTP_DB_USER', 'postgres'),
        password=os.getenv('OLTP_DB_PASSWORD', 'postgres123'),
        database=os.getenv('OLTP_DB_NAME', 'pivot_studio')
    )


def create_dw_schema(**context):
    """Create DW schema if not exists."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {{DW_SCHEMA}}")
        conn.commit()
        print(f"Schema {{DW_SCHEMA}} created/verified")
    finally:
        cur.close()
        conn.close()


def create_dimension_tables(**context):
    """Create dimension tables if not exist."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for dim_table in DIMENSION_TABLES:
            dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
            full_table = f"{{DW_SCHEMA}}.{{dim_name}}"
            
            # Find columns for this dimension from mappings
            dim_mappings = [m for m in MAPPINGS if m.get('target_table') == dim_name]
            
            columns = ["id SERIAL PRIMARY KEY"]
            for m in dim_mappings:
                col_name = m.get('target_column', '')
                if col_name and col_name != 'id':
                    columns.append(f"{{col_name}} VARCHAR(255)")
            columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
            
            create_sql = f\"\"\"
                CREATE TABLE IF NOT EXISTS {{full_table}} (
                    {{', '.join(columns)}}
                )
            \"\"\"
            cur.execute(create_sql)
            print(f"Table {{full_table}} created/verified")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error creating dimension tables: {{e}}")
        raise
    finally:
        cur.close()
        conn.close()


def create_fact_table(**context):
    """Create fact table if not exists."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        fact_name = FACT_TABLE.split('.')[-1] if '.' in FACT_TABLE else FACT_TABLE
        full_table = f"{{DW_SCHEMA}}.{{fact_name}}"
        
        # Find columns for fact table from mappings
        fact_mappings = [m for m in MAPPINGS if m.get('target_table') == fact_name]
        
        columns = ["id SERIAL PRIMARY KEY"]
        
        # Add FK columns for each dimension
        for dim_table in DIMENSION_TABLES:
            dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
            columns.append(f"{{dim_name}}_id INTEGER")
        
        # Add measure columns
        for m in fact_mappings:
            col_name = m.get('target_column', '')
            if col_name and col_name != 'id':
                columns.append(f"{{col_name}} NUMERIC(20,4)")
        
        columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
        
        create_sql = f\"\"\"
            CREATE TABLE IF NOT EXISTS {{full_table}} (
                {{', '.join(columns)}}
            )
        \"\"\"
        cur.execute(create_sql)
        print(f"Table {{full_table}} created/verified")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error creating fact table: {{e}}")
        raise
    finally:
        cur.close()
        conn.close()


def sync_dimension(dim_table: str, **context):
    """Sync a dimension table from source."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Find mappings for this dimension
        dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
        dim_mappings = [m for m in MAPPINGS if m.get('target_table') == dim_name]
        
        if not dim_mappings:
            print(f"No mappings found for {{dim_table}}, skipping")
            return 0
        
        # Build columns
        source_cols = []
        target_cols = []
        for m in dim_mappings:
            source_expr = m.get('transformation') or f"{{m['source_table']}}.{{m['source_column']}}"
            source_cols.append(f"{{source_expr}} AS {{m['target_column']}}")
            target_cols.append(m['target_column'])
        
        # Get unique source tables
        source_tables = list(set([m['source_table'] for m in dim_mappings]))
        from_clause = ", ".join(source_tables)
        
        # Full table name
        full_table = f"{{DW_SCHEMA}}.{{dim_name}}"
        
        # Generate INSERT query
        insert_sql = f\"\"\"
            INSERT INTO {{full_table}} ({{', '.join(target_cols)}})
            SELECT DISTINCT {{', '.join(source_cols)}}
            FROM {{from_clause}}
            ON CONFLICT DO NOTHING
        \"\"\"
        
        cur.execute(insert_sql)
        rows = cur.rowcount
        conn.commit()
        print(f"Inserted {{rows}} rows into {{full_table}}")
        return rows
        
    except Exception as e:
        conn.rollback()
        print(f"Error syncing {{dim_table}}: {{e}}")
        raise
    finally:
        cur.close()
        conn.close()


def sync_fact_table(**context):
    """Sync fact table from source."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        fact_name = FACT_TABLE.split('.')[-1] if '.' in FACT_TABLE else FACT_TABLE
        fact_mappings = [m for m in MAPPINGS if m.get('target_table') == fact_name]
        
        if not fact_mappings:
            print(f"No mappings found for fact table, skipping")
            return 0
        
        # Build columns
        source_cols = []
        target_cols = []
        for m in fact_mappings:
            source_expr = m.get('transformation') or f"{{m['source_table']}}.{{m['source_column']}}"
            source_cols.append(f"{{source_expr}} AS {{m['target_column']}}")
            target_cols.append(m['target_column'])
        
        source_tables = list(set([m['source_table'] for m in fact_mappings]))
        from_clause = ", ".join(source_tables)
        full_table = f"{{DW_SCHEMA}}.{{fact_name}}"
        
        insert_sql = f\"\"\"
            INSERT INTO {{full_table}} ({{', '.join(target_cols)}})
            SELECT {{', '.join(source_cols)}}
            FROM {{from_clause}}
        \"\"\"
        
        cur.execute(insert_sql)
        rows = cur.rowcount
        conn.commit()
        print(f"Inserted {{rows}} rows into {{full_table}}")
        return rows
        
    except Exception as e:
        conn.rollback()
        print(f"Error syncing fact table: {{e}}")
        raise
    finally:
        cur.close()
        conn.close()


# Create DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f'ETL Pipeline for {{CUBE_NAME}}',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'olap', 'cube'],
) as dag:
    
    # Task 1: Create DW Schema
    create_schema = PythonOperator(
        task_id='create_dw_schema',
        python_callable=create_dw_schema,
    )
    
    # Task 2: Create Dimension Tables (DDL)
    create_dims = PythonOperator(
        task_id='create_dimension_tables',
        python_callable=create_dimension_tables,
    )
    
    # Task 3: Create Fact Table (DDL)
    create_fact = PythonOperator(
        task_id='create_fact_table',
        python_callable=create_fact_table,
    )
    
    # Task 4: Sync Dimension Tables (ETL)
    dim_tasks = []
    for dim_table in DIMENSION_TABLES:
        dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
        task = PythonOperator(
            task_id=f'sync_dim_{{dim_name}}',
            python_callable=sync_dimension,
            op_kwargs={{'dim_table': dim_table}},
        )
        dim_tasks.append(task)
    
    # Task 5: Sync Fact Table (ETL)
    sync_fact = PythonOperator(
        task_id='sync_fact_table',
        python_callable=sync_fact_table,
    )
    
    # Set dependencies: schema -> create tables -> sync dims -> sync fact
    create_schema >> create_dims >> create_fact
    if dim_tasks:
        create_fact >> dim_tasks >> sync_fact
    else:
        create_fact >> sync_fact
'''
        
        return dag_code
    
    def _sanitize_dag_id(self, name: str) -> str:
        """Convert cube name to valid DAG ID."""
        # Replace non-alphanumeric with underscore
        import re
        dag_id = re.sub(r'[^a-zA-Z0-9가-힣_]', '_', name)
        return f"etl_{dag_id}"
    
    def save_dag(self, etl_config: Dict) -> DAGInfo:
        """Generate and save DAG file."""
        cube_name = etl_config.get("cube_name", "unnamed")
        dag_id = self._sanitize_dag_id(cube_name)
        
        # Generate DAG code
        dag_code = self.generate_dag_code(etl_config)
        
        # Ensure dags folder exists
        DAGS_FOLDER.mkdir(parents=True, exist_ok=True)
        
        # Save to file
        file_path = DAGS_FOLDER / f"{dag_id}.py"
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(dag_code)
        
        print(f"Saved DAG to {file_path}")
        
        return DAGInfo(
            dag_id=dag_id,
            file_path=str(file_path),
            cube_name=cube_name,
            created_at=datetime.now().isoformat(),
            airflow_url=f"{self.airflow_host}/dags/{dag_id}/grid"
        )
    
    async def trigger_dag(self, dag_id: str) -> Dict:
        """Trigger a DAG run via Airflow REST API."""
        url = f"{self.airflow_host}/api/v1/dags/{dag_id}/dagRuns"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json={"conf": {}},
                auth=self.auth,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return {
                    "success": True,
                    "dag_run_id": data.get("dag_run_id"),
                    "execution_date": data.get("execution_date"),
                    "state": data.get("state"),
                    "url": f"{self.airflow_host}/dags/{dag_id}/grid"
                }
            else:
                return {
                    "success": False,
                    "error": response.text,
                    "status_code": response.status_code
                }
    
    async def get_dag_status(self, dag_id: str) -> Dict:
        """Get DAG status and recent runs."""
        url = f"{self.airflow_host}/api/v1/dags/{dag_id}/dagRuns"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                auth=self.auth,
                params={"limit": 5, "order_by": "-execution_date"}
            )
            
            if response.status_code == 200:
                data = response.json()
                runs = data.get("dag_runs", [])
                return {
                    "success": True,
                    "dag_id": dag_id,
                    "total_runs": data.get("total_entries", 0),
                    "recent_runs": [
                        {
                            "run_id": r.get("dag_run_id"),
                            "state": r.get("state"),
                            "execution_date": r.get("execution_date"),
                            "start_date": r.get("start_date"),
                            "end_date": r.get("end_date")
                        }
                        for r in runs
                    ],
                    "url": f"{self.airflow_host}/dags/{dag_id}/grid"
                }
            else:
                return {
                    "success": False,
                    "error": response.text
                }
    
    async def check_airflow_health(self) -> Dict:
        """Check if Airflow is accessible."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    f"{self.airflow_host}/health",
                    auth=self.auth
                )
                
                if response.status_code == 200:
                    return {"status": "healthy", "url": self.airflow_host}
                else:
                    return {"status": "unhealthy", "error": response.text}
        except Exception as e:
            return {"status": "unavailable", "error": str(e)}
    
    def list_dags(self) -> List[Dict]:
        """List all generated DAGs."""
        dags = []
        if DAGS_FOLDER.exists():
            for f in DAGS_FOLDER.glob("etl_*.py"):
                dags.append({
                    "dag_id": f.stem,
                    "file_path": str(f),
                    "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat()
                })
        return dags
    
    def delete_dag(self, dag_id: str) -> bool:
        """Delete a DAG file."""
        file_path = DAGS_FOLDER / f"{dag_id}.py"
        if file_path.exists():
            file_path.unlink()
            return True
        return False


# Singleton instance
airflow_service = AirflowService()

