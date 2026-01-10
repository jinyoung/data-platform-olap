"""
Auto-generated Airflow DAG for ETL Pipeline
Cube: 다음SQL
Generated: 2026-01-09T14:13:05.867445
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import os

# DAG Configuration
DAG_ID = "etl_다음SQL"
CUBE_NAME = "다음SQL"
FACT_TABLE = "dw.fact___sql"
DW_SCHEMA = "dw"
SYNC_MODE = "incremental"
INCREMENTAL_COLUMN = 'updated_at'

DIMENSION_TABLES = []
SOURCE_TABLES = ["rwis.rdf01hh_tb", "all_objects", "rwis.rditag_tb"]
MAPPINGS = [
        {
                "source_table": "rwis",
                "source_column": "rdf01hh_tb.VAL",
                "target_table": "fact_average_flow",
                "target_column": "average_flow",
                "transformation": "AVG(rwis.rdf01hh_tb.VAL)"
        },
        {
                "source_table": "rwis",
                "source_column": "rdf01hh_tb.LOG_TIME",
                "target_table": "dim_time",
                "target_column": "date",
                "transformation": "CAST(rwis.rdf01hh_tb.LOG_TIME AS DATE)"
        },
        {
                "source_table": "rwis",
                "source_column": "rditag_tb.TAG_SN",
                "target_table": "dim_tag",
                "target_column": "tag_id",
                "transformation": "rwis.rditag_tb.TAG_SN"
        }
]

# Default arguments
default_args = {
    'owner': 'olap-etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


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
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DW_SCHEMA}")
        conn.commit()
        print(f"Schema {DW_SCHEMA} created/verified")
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
            print(f"No mappings found for {dim_table}, skipping")
            return 0
        
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
        
        # Full table name
        full_table = f"{DW_SCHEMA}.{dim_name}"
        
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
        print(f"Inserted {rows} rows into {full_table}")
        return rows
        
    except Exception as e:
        conn.rollback()
        print(f"Error syncing {dim_table}: {e}")
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
            source_expr = m.get('transformation') or f"{m['source_table']}.{m['source_column']}"
            source_cols.append(f"{source_expr} AS {m['target_column']}")
            target_cols.append(m['target_column'])
        
        source_tables = list(set([m['source_table'] for m in fact_mappings]))
        from_clause = ", ".join(source_tables)
        full_table = f"{DW_SCHEMA}.{fact_name}"
        
        insert_sql = f"""
            INSERT INTO {full_table} ({', '.join(target_cols)})
            SELECT {', '.join(source_cols)}
            FROM {from_clause}
        """
        
        cur.execute(insert_sql)
        rows = cur.rowcount
        conn.commit()
        print(f"Inserted {rows} rows into {full_table}")
        return rows
        
    except Exception as e:
        conn.rollback()
        print(f"Error syncing fact table: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# Create DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f'ETL Pipeline for {CUBE_NAME}',
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
    
    # Task 2: Sync Dimension Tables
    dim_tasks = []
    for dim_table in DIMENSION_TABLES:
        dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
        task = PythonOperator(
            task_id=f'sync_dim_{dim_name}',
            python_callable=sync_dimension,
            op_kwargs={'dim_table': dim_table},
        )
        dim_tasks.append(task)
    
    # Task 3: Sync Fact Table
    sync_fact = PythonOperator(
        task_id='sync_fact_table',
        python_callable=sync_fact_table,
    )
    
    # Set dependencies: schema -> dimensions -> fact
    create_schema >> dim_tasks >> sync_fact
