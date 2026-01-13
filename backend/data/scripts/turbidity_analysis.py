import os
import psycopg2
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경변수에서 DB 연결 정보 읽기 (실행 시 자동 주입됨)
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


SYNC_MODE = os.environ.get('ETL_SYNC_MODE', 'full').strip().lower()
ETL_LAST_SYNC = os.environ.get('ETL_LAST_SYNC', '1900-01-01 00:00:00')
ETL_BATCH_ID = os.environ.get('ETL_BATCH_ID') or datetime.utcnow().strftime('%Y%m%d%H%M%S')


def create_schema(conn):
    """Create DW schema if it does not exist."""
    with conn.cursor() as cursor:
        cursor.execute('CREATE SCHEMA IF NOT EXISTS dw;')
    logging.info("Schema ensured: dw")


def drop_existing_tables(conn):
    """Drop existing DW objects for full reload."""
    if SYNC_MODE != 'full':
        return
    logging.info("Full mode: dropping existing DW tables/materialized views...")
    with conn.cursor() as cursor:
        cursor.execute('DROP MATERIALIZED VIEW IF EXISTS dw.mv_turbidity_by_site CASCADE;')
        cursor.execute('DROP MATERIALIZED VIEW IF EXISTS dw.mv_turbidity_by_time CASCADE;')
        cursor.execute('DROP TABLE IF EXISTS dw.fact_turbidity CASCADE;')
        cursor.execute('DROP TABLE IF EXISTS dw.dim_time CASCADE;')
        cursor.execute('DROP TABLE IF EXISTS dw.dim_site CASCADE;')
        cursor.execute('DROP TABLE IF EXISTS dw.dim_tag CASCADE;')
    logging.info("Dropped existing DW objects (if existed).")


def create_dimension_tables(conn):
    """Create dimension tables (DDL)."""
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dw.dim_time (
                id SERIAL PRIMARY KEY,
                date VARCHAR(255) NOT NULL,
                year VARCHAR(255) NOT NULL,
                quarter VARCHAR(255) NOT NULL,
                month VARCHAR(255) NOT NULL,
                day VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT uq_dim_time_date UNIQUE (date)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dw.dim_site (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT uq_dim_site_name UNIQUE (name)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dw.dim_tag (
                id SERIAL PRIMARY KEY,
                description VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT uq_dim_tag_description UNIQUE (description)
            );
        """)
    logging.info("Dimension tables ensured: dim_time, dim_site, dim_tag")


def create_fact_table(conn):
    """Create fact table (DDL) with required FK columns."""
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dw.fact_turbidity (
                dim_time_id INTEGER NOT NULL REFERENCES dw.dim_time(id),
                dim_site_id INTEGER NOT NULL REFERENCES dw.dim_site(id),
                dim_tag_id INTEGER NOT NULL REFERENCES dw.dim_tag(id),

                avg_turbidity NUMERIC,
                max_turbidity NUMERIC,
                min_turbidity NUMERIC,
                total_measurements INTEGER,

                etl_batch_id VARCHAR(50),
                loaded_at TIMESTAMP DEFAULT NOW(),

                CONSTRAINT uq_fact_turbidity UNIQUE (dim_time_id, dim_site_id, dim_tag_id)
            );
        """)
    logging.info("Fact table ensured: fact_turbidity")


def ensure_tables_if_not_exists(conn):
    """Ensure schema and required tables exist (for incremental mode too)."""
    create_schema(conn)
    create_dimension_tables(conn)
    create_fact_table(conn)


def load_dim_time_full(conn):
    """
    Load dim_time from RWIS.RDF01HH_TB (distinct LOG_TIME).
    LOG_TIME is like YYYYMMDDHH -> dim_time.date stores LOG_TIME as-is to keep join stable.
    """
    sql = """
        WITH src AS (
            SELECT DISTINCT f."LOG_TIME" AS log_time_str
            FROM "RWIS"."RDF01HH_TB" f
            WHERE f."LOG_TIME" IS NOT NULL
        )
        INSERT INTO dw.dim_time (date, year, quarter, month, day, created_at, updated_at)
        SELECT
            s.log_time_str AS date,
            SUBSTRING(s.log_time_str FROM 1 FOR 4) AS year,
            (CASE
                WHEN CAST(SUBSTRING(s.log_time_str FROM 5 FOR 2) AS INTEGER) BETWEEN 1 AND 3 THEN '1'
                WHEN CAST(SUBSTRING(s.log_time_str FROM 5 FOR 2) AS INTEGER) BETWEEN 4 AND 6 THEN '2'
                WHEN CAST(SUBSTRING(s.log_time_str FROM 5 FOR 2) AS INTEGER) BETWEEN 7 AND 9 THEN '3'
                WHEN CAST(SUBSTRING(s.log_time_str FROM 5 FOR 2) AS INTEGER) BETWEEN 10 AND 12 THEN '4'
                ELSE NULL
             END) AS quarter,
            SUBSTRING(s.log_time_str FROM 5 FOR 2) AS month,
            SUBSTRING(s.log_time_str FROM 7 FOR 2) AS day,
            NOW(), NOW()
        FROM src s
        ON CONFLICT (date) DO UPDATE SET
            year = EXCLUDED.year,
            quarter = EXCLUDED.quarter,
            month = EXCLUDED.month,
            day = EXCLUDED.day,
            updated_at = NOW();
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rowcount = cursor.rowcount
    logging.info(f"Loaded dim_time (upsert). rowcount={rowcount}")


def load_dim_site_full(conn):
    """
    Load dim_site from RWIS.RDISAUP_TB.
    Uses BPLC_NM as 'name' per mapping and cube schema requirement.
    """
    sql = """
        WITH src AS (
            SELECT DISTINCT a."BPLC_NM" AS name
            FROM "RWIS"."RDISAUP_TB" a
            WHERE a."BPLC_NM" IS NOT NULL
        )
        INSERT INTO dw.dim_site (name, created_at, updated_at)
        SELECT s.name, NOW(), NOW()
        FROM src s
        ON CONFLICT (name) DO UPDATE SET
            updated_at = NOW();
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rowcount = cursor.rowcount
    logging.info(f"Loaded dim_site (upsert). rowcount={rowcount}")


def load_dim_tag_full(conn):
    """
    Load dim_tag from RWIS.RDITAG_TB.
    Uses TAG_DESC as 'description' per mapping and cube schema requirement.
    """
    sql = """
        WITH src AS (
            SELECT DISTINCT t."TAG_DESC" AS description
            FROM "RWIS"."RDITAG_TB" t
            WHERE t."TAG_DESC" IS NOT NULL
        )
        INSERT INTO dw.dim_tag (description, created_at, updated_at)
        SELECT s.description, NOW(), NOW()
        FROM src s
        ON CONFLICT (description) DO UPDATE SET
            updated_at = NOW();
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rowcount = cursor.rowcount
    logging.info(f"Loaded dim_tag (upsert). rowcount={rowcount}")


def load_dimensions_full(conn):
    """Load all dimensions for full mode (using upsert for safety)."""
    load_dim_time_full(conn)
    load_dim_site_full(conn)
    load_dim_tag_full(conn)


def load_dimensions_incremental(conn):
    """
    Incremental dimension load.
    For simplicity and correctness, uses same upsert queries as full mode.
    """
    logging.info(f"Incremental dimension load starting (ETL_LAST_SYNC={ETL_LAST_SYNC})...")
    load_dimensions_full(conn)


def load_fact_full(conn):
    """
    Load fact_turbidity:
    - Sources: RWIS.RDF01HH_TB f, RWIS.RDITAG_TB t, RWIS.RDISAUP_TB a
    - Dimension joins:
        dim_time join on dim_time.date = f.LOG_TIME
        dim_site join on dim_site.name = a.BPLC_NM (via t.SMS_CODE = a.BPLC_CODE)
        dim_tag join on dim_tag.description = t.TAG_DESC
    - Measures per config: AVG/MAX/MIN of f.VAL numeric and COUNT non-null.
    """
    sql = """
        INSERT INTO dw.fact_turbidity (
            dim_time_id, dim_site_id, dim_tag_id,
            avg_turbidity, max_turbidity, min_turbidity, total_measurements,
            etl_batch_id, loaded_at
        )
        SELECT
            dt.id AS dim_time_id,
            ds.id AS dim_site_id,
            dg.id AS dim_tag_id,
            AVG(CAST(f."VAL" AS NUMERIC)) AS avg_turbidity,
            MAX(CAST(f."VAL" AS NUMERIC)) AS max_turbidity,
            MIN(CAST(f."VAL" AS NUMERIC)) AS min_turbidity,
            COUNT(CASE WHEN f."VAL" IS NOT NULL THEN 1 END) AS total_measurements,
            %s AS etl_batch_id,
            NOW() AS loaded_at
        FROM "RWIS"."RDF01HH_TB" f
        JOIN "RWIS"."RDITAG_TB" t
            ON t."TAGSN" = f."TAGSN"
        JOIN "RWIS"."RDISAUP_TB" a
            ON a."BPLC_CODE" = t."SMS_CODE"
        JOIN dw.dim_time dt
            ON dt.date = f."LOG_TIME"
        JOIN dw.dim_site ds
            ON ds.name = a."BPLC_NM"
        JOIN dw.dim_tag dg
            ON dg.description = t."TAG_DESC"
        WHERE f."LOG_TIME" IS NOT NULL
          AND f."VAL" IS NOT NULL
        GROUP BY
            dt.id, ds.id, dg.id
        ON CONFLICT (dim_time_id, dim_site_id, dim_tag_id) DO UPDATE SET
            avg_turbidity = EXCLUDED.avg_turbidity,
            max_turbidity = EXCLUDED.max_turbidity,
            min_turbidity = EXCLUDED.min_turbidity,
            total_measurements = EXCLUDED.total_measurements,
            etl_batch_id = EXCLUDED.etl_batch_id,
            loaded_at = NOW();
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (ETL_BATCH_ID,))
        rowcount = cursor.rowcount
    logging.info(f"Loaded fact_turbidity (upsert). rowcount={rowcount}")


def load_fact_incremental(conn):
    """
    Incremental fact load:
    - Filters source by LOG_TIME >= ETL_LAST_SYNC (interpreted as timestamp) converted to YYYYMMDDHH.
    - Upserts into fact on (dim_time_id, dim_site_id, dim_tag_id).
    """
    last_sync_ts = ETL_LAST_SYNC
    sql = """
        WITH params AS (
            SELECT
              to_char((%s)::timestamp, 'YYYYMMDDHH24') AS last_sync_log_time
        )
        INSERT INTO dw.fact_turbidity (
            dim_time_id, dim_site_id, dim_tag_id,
            avg_turbidity, max_turbidity, min_turbidity, total_measurements,
            etl_batch_id, loaded_at
        )
        SELECT
            dt.id AS dim_time_id,
            ds.id AS dim_site_id,
            dg.id AS dim_tag_id,
            AVG(CAST(f."VAL" AS NUMERIC)) AS avg_turbidity,
            MAX(CAST(f."VAL" AS NUMERIC)) AS max_turbidity,
            MIN(CAST(f."VAL" AS NUMERIC)) AS min_turbidity,
            COUNT(CASE WHEN f."VAL" IS NOT NULL THEN 1 END) AS total_measurements,
            %s AS etl_batch_id,
            NOW() AS loaded_at
        FROM params p
        JOIN "RWIS"."RDF01HH_TB" f
            ON f."LOG_TIME" >= p.last_sync_log_time
        JOIN "RWIS"."RDITAG_TB" t
            ON t."TAGSN" = f."TAGSN"
        JOIN "RWIS"."RDISAUP_TB" a
            ON a."BPLC_CODE" = t."SMS_CODE"
        JOIN dw.dim_time dt
            ON dt.date = f."LOG_TIME"
        JOIN dw.dim_site ds
            ON ds.name = a."BPLC_NM"
        JOIN dw.dim_tag dg
            ON dg.description = t."TAG_DESC"
        WHERE f."LOG_TIME" IS NOT NULL
          AND f."VAL" IS NOT NULL
        GROUP BY
            dt.id, ds.id, dg.id
        ON CONFLICT (dim_time_id, dim_site_id, dim_tag_id) DO UPDATE SET
            avg_turbidity = EXCLUDED.avg_turbidity,
            max_turbidity = EXCLUDED.max_turbidity,
            min_turbidity = EXCLUDED.min_turbidity,
            total_measurements = EXCLUDED.total_measurements,
            etl_batch_id = EXCLUDED.etl_batch_id,
            loaded_at = NOW();
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (last_sync_ts, ETL_BATCH_ID))
        rowcount = cursor.rowcount
    logging.info(f"Incremental loaded fact_turbidity (upsert). rowcount={rowcount}")


def create_indexes(conn):
    """Create required indexes for OLAP performance (FKs and dimension join keys)."""
    with conn.cursor() as cursor:
        # Fact FK indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_turbidity_dim_time_id ON dw.fact_turbidity(dim_time_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_turbidity_dim_site_id ON dw.fact_turbidity(dim_site_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_turbidity_dim_tag_id ON dw.fact_turbidity(dim_tag_id);")

        # Composite index
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_turbidity_composite ON dw.fact_turbidity(dim_time_id, dim_site_id);")

        # Dimension join key indexes (unique constraints already create indexes, but explicit is fine)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dim_time_date ON dw.dim_time(date);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dim_site_name ON dw.dim_site(name);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dim_tag_description ON dw.dim_tag(description);")
    logging.info("Indexes ensured.")


def create_materialized_views(conn):
    """Create materialized views for common aggregations and their indexes."""
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS dw.mv_turbidity_by_site AS
            SELECT
                dim_site_id,
                AVG(avg_turbidity) AS avg_of_avg_turbidity,
                MAX(max_turbidity) AS max_turbidity,
                MIN(min_turbidity) AS min_turbidity,
                SUM(total_measurements) AS total_measurements
            FROM dw.fact_turbidity
            GROUP BY dim_site_id;
        """)

        cursor.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS dw.mv_turbidity_by_time AS
            SELECT
                dim_time_id,
                AVG(avg_turbidity) AS avg_of_avg_turbidity,
                MAX(max_turbidity) AS max_turbidity,
                MIN(min_turbidity) AS min_turbidity,
                SUM(total_measurements) AS total_measurements
            FROM dw.fact_turbidity
            GROUP BY dim_time_id;
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mv_turbidity_by_site_dim_site_id ON dw.mv_turbidity_by_site(dim_site_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mv_turbidity_by_time_dim_time_id ON dw.mv_turbidity_by_time(dim_time_id);")
    logging.info("Materialized views ensured.")


def refresh_materialized_views(conn):
    """Refresh materialized views."""
    with conn.cursor() as cursor:
        cursor.execute("REFRESH MATERIALIZED VIEW dw.mv_turbidity_by_site;")
        cursor.execute("REFRESH MATERIALIZED VIEW dw.mv_turbidity_by_time;")
    logging.info("Materialized views refreshed.")


def main():
    """Main ETL entrypoint for turbidity_analysis cube."""
    conn = None
    try:
        if SYNC_MODE not in ('full', 'incremental'):
            raise ValueError(f"Invalid ETL_SYNC_MODE='{SYNC_MODE}'. Use 'full' or 'incremental'.")

        logging.info(f"ETL start: cube=turbidity_analysis mode={SYNC_MODE} batch_id={ETL_BATCH_ID}")

        conn = get_connection()
        conn.autocommit = False

        create_schema(conn)

        if SYNC_MODE == 'full':
            logging.info("=== FULL RELOAD MODE ===")
            drop_existing_tables(conn)
            create_schema(conn)
            create_dimension_tables(conn)
            create_fact_table(conn)
            load_dimensions_full(conn)
            load_fact_full(conn)
        else:
            logging.info("=== INCREMENTAL MODE ===")
            ensure_tables_if_not_exists(conn)
            load_dimensions_incremental(conn)
            load_fact_incremental(conn)

        create_indexes(conn)
        create_materialized_views(conn)
        refresh_materialized_views(conn)

        conn.commit()
        logging.info(f"ETL completed successfully. mode={SYNC_MODE} batch_id={ETL_BATCH_ID}")

    except Exception as e:
        if conn is not None:
            conn.rollback()
        logging.error(f"ETL error: {e}", exc_info=True)
        raise
    finally:
        if conn is not None:
            conn.close()
            logging.info("DB connection closed.")


if __name__ == "__main__":
    main()