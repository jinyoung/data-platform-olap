"""
Auto-generated Airflow DAG for ETL Pipeline
Cube: turbidity_analysis
Generated: 2026-01-12T10:00:05.205268
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import os

# DAG Configuration
DAG_ID = "etl_turbidity_analysis"
CUBE_NAME = "turbidity_analysis"
FACT_TABLE = "dw.fact_turbidity"
DW_SCHEMA = "dw"
SYNC_MODE = "incremental"
INCREMENTAL_COLUMN = 'LOG_TIME'

DIMENSION_TABLES = ["dw.dim_time", "dw.dim_site", "dw.dim_tag"]
SOURCE_TABLES = ["RWIS.RDF01HH_TB", "RWIS.RDITAG_TB", "RWIS.RDISAUP_TB"]
MAPPINGS = [
        {
                "source_table": "RWIS",
                "source_column": "RDF01HH_TB.VAL",
                "target_table": "fact_turbidity",
                "target_column": "avg_turbidity",
                "transformation": "AVG(...)"
        },
        {
                "source_table": "RWIS",
                "source_column": "RDF01HH_TB.VAL",
                "target_table": "fact_turbidity",
                "target_column": "max_turbidity",
                "transformation": "MAX(...)"
        },
        {
                "source_table": "RWIS",
                "source_column": "RDF01HH_TB.VAL",
                "target_table": "fact_turbidity",
                "target_column": "min_turbidity",
                "transformation": "MIN(...)"
        },
        {
                "source_table": "RWIS",
                "source_column": "RDF01HH_TB.VAL",
                "target_table": "fact_turbidity",
                "target_column": "total_measurements",
                "transformation": "COUNT(...)"
        },
        {
                "source_table": "RWIS",
                "source_column": "RDISAUP_TB.BPLC_CODE",
                "target_table": "dim_site",
                "target_column": "id",
                "transformation": ""
        },
        {
                "source_table": "RWIS",
                "source_column": "RDISAUP_TB.BPLC_NM",
                "target_table": "dim_site",
                "target_column": "name",
                "transformation": ""
        },
        {
                "source_table": "RWIS",
                "source_column": "RDITAG_TB.TAGSN",
                "target_table": "dim_tag",
                "target_column": "id",
                "transformation": ""
        },
        {
                "source_table": "RWIS",
                "source_column": "RDITAG_TB.TAG_DESC",
                "target_table": "dim_tag",
                "target_column": "description",
                "transformation": ""
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
        database=os.getenv('OLTP_DB_NAME', 'meetingroom')
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


def create_dimension_tables(**context):
    """Create dimension tables if not exist."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for dim_table in DIMENSION_TABLES:
            dim_name = dim_table.split('.')[-1] if '.' in dim_table else dim_table
            full_table = f"{DW_SCHEMA}.{dim_name}"
            
            # Find columns for this dimension from mappings
            dim_mappings = [m for m in MAPPINGS if m.get('target_table') == dim_name]
            
            columns = ["id SERIAL PRIMARY KEY"]
            for m in dim_mappings:
                col_name = m.get('target_column', '')
                if col_name and col_name != 'id':
                    columns.append(f"{col_name} VARCHAR(255)")
            columns.append("_etl_loaded_at TIMESTAMP DEFAULT NOW()")
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                    {', '.join(columns)}
                )
            """
            cur.execute(create_sql)
            print(f"Table {full_table} created/verified")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error creating dimension tables: {e}")
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
        full_table = f"{DW_SCHEMA}.{fact_name}"
        
        # Find columns for fact table from mappings
        fact_mappings = [m for m in MAPPINGS if m.get('target_table') == fact_name]
        
        columns = ["id SERIAL PRIMARY KEY"]
        
        # Add FK columns for each dimension
        for dim_table in DIMENSION_TABLES:
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
        print(f"Table {full_table} created/verified")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error creating fact table: {e}")
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
            task_id=f'sync_dim_{dim_name}',
            python_callable=sync_dimension,
            op_kwargs={'dim_table': dim_table},
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
