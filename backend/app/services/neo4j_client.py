"""Neo4j client for connecting to robo-analyzer's Neo4j database."""
from typing import Any, Dict, List, Optional
from neo4j import AsyncGraphDatabase

from ..core.config import get_settings


class Neo4jClient:
    """Neo4j async client for fetching table catalogs."""
    
    def __init__(
        self,
        uri: str = None,
        user: str = None,
        password: str = None,
        database: str = None
    ):
        settings = get_settings()
        self.uri = uri or settings.neo4j_uri
        self.user = user or settings.neo4j_user
        self.password = password or settings.neo4j_password
        self.database = database or settings.neo4j_database
        self._driver = None
    
    async def connect(self):
        """Initialize the driver connection."""
        if self._driver is None:
            self._driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password)
            )
    
    async def close(self):
        """Close the driver connection."""
        if self._driver:
            await self._driver.close()
            self._driver = None
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute a Cypher query and return results."""
        if self._driver is None:
            await self.connect()
        
        async with self._driver.session(database=self.database) as session:
            result = await session.run(query, params or {})
            return await result.data()
    
    async def get_tables(
        self,
        user_id: str = None,
        project_name: str = None,
        schema: str = None,
        search: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get table list from Neo4j catalog.
        
        Returns tables with their columns and relationships.
        """
        where_conditions = []
        
        if user_id:
            where_conditions.append(f"t.user_id = '{user_id}'")
        if project_name:
            where_conditions.append(f"t.project_name = '{project_name}'")
        if schema:
            where_conditions.append(f"t.schema = '{schema}'")
        if search:
            where_conditions.append(
                f"(toLower(t.name) CONTAINS toLower('{search}') "
                f"OR toLower(t.description) CONTAINS toLower('{search}'))"
            )
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "true"
        
        query = f"""
            MATCH (t:Table)
            WHERE {where_clause}
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            WITH t, collect({{
                name: c.name,
                dtype: c.dtype,
                nullable: c.nullable,
                description: c.description
            }}) AS columns
            RETURN t.name AS name,
                   t.schema AS schema,
                   t.description AS description,
                   t.table_type AS table_type,
                   t.project_name AS project_name,
                   t.user_id AS user_id,
                   columns
            ORDER BY t.schema, t.name
            LIMIT {limit}
        """
        
        return await self.execute_query(query)
    
    async def get_table_columns(
        self,
        table_name: str,
        schema: str = None,
        user_id: str = None,
        project_name: str = None
    ) -> List[Dict]:
        """Get columns for a specific table."""
        where_conditions = [f"t.name = '{table_name}'"]
        
        if schema:
            where_conditions.append(f"t.schema = '{schema}'")
        if user_id:
            where_conditions.append(f"t.user_id = '{user_id}'")
        if project_name:
            where_conditions.append(f"t.project_name = '{project_name}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            WHERE {where_clause}
            RETURN c.name AS name,
                   c.dtype AS dtype,
                   c.nullable AS nullable,
                   c.description AS description,
                   c.fqn AS fqn
            ORDER BY c.name
        """
        
        return await self.execute_query(query)
    
    async def get_table_relationships(
        self,
        user_id: str = None,
        project_name: str = None
    ) -> List[Dict]:
        """Get foreign key relationships between tables."""
        where_conditions = []
        
        if user_id:
            where_conditions.append(f"t1.user_id = '{user_id}'")
        if project_name:
            where_conditions.append(f"t1.project_name = '{project_name}'")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "true"
        
        query = f"""
            MATCH (t1:Table)-[r:FK_TO_TABLE]->(t2:Table)
            WHERE {where_clause}
            RETURN t1.name AS from_table,
                   t1.schema AS from_schema,
                   r.from_column AS from_column,
                   t2.name AS to_table,
                   t2.schema AS to_schema,
                   r.to_column AS to_column,
                   type(r) AS relationship_type
            ORDER BY from_table, to_table
        """
        
        return await self.execute_query(query)
    
    async def get_schemas(
        self,
        user_id: str = None,
        project_name: str = None
    ) -> List[str]:
        """Get list of unique schemas."""
        where_conditions = []
        
        if user_id:
            where_conditions.append(f"t.user_id = '{user_id}'")
        if project_name:
            where_conditions.append(f"t.project_name = '{project_name}'")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "true"
        
        query = f"""
            MATCH (t:Table)
            WHERE {where_clause} AND t.schema IS NOT NULL AND t.schema <> ''
            RETURN DISTINCT t.schema AS schema
            ORDER BY schema
        """
        
        results = await self.execute_query(query)
        return [r["schema"] for r in results]
    
    async def register_olap_table(
        self,
        table_name: str,
        schema: str,
        columns: List[Dict],
        source_tables: List[str],
        user_id: str,
        project_name: str,
        cube_name: str
    ) -> Dict:
        """Register OLAP table in Neo4j and create lineage relationships.
        
        This creates:
        1. Table node for the OLAP table
        2. Column nodes for each column
        3. DATA_FLOW_TO relationships from source tables
        """
        queries = []
        
        # Create OLAP Table node
        queries.append(f"""
            MERGE (t:Table {{
                user_id: '{user_id}',
                project_name: '{project_name}',
                schema: '{schema}',
                name: '{table_name}'
            }})
            SET t.table_type = 'OLAP',
                t.cube_name = '{cube_name}',
                t.description = 'OLAP Star Schema Table for {cube_name}'
            RETURN t
        """)
        
        # Create Column nodes
        for col in columns:
            col_name = col.get("name", "")
            col_dtype = col.get("dtype", "VARCHAR")
            col_desc = col.get("description", "")
            fqn = f"{schema}.{table_name}.{col_name}".lower()
            
            queries.append(f"""
                MATCH (t:Table {{
                    user_id: '{user_id}',
                    project_name: '{project_name}',
                    schema: '{schema}',
                    name: '{table_name}'
                }})
                MERGE (c:Column {{
                    user_id: '{user_id}',
                    project_name: '{project_name}',
                    fqn: '{fqn}'
                }})
                SET c.name = '{col_name}',
                    c.dtype = '{col_dtype}',
                    c.description = '{col_desc}'
                MERGE (t)-[:HAS_COLUMN]->(c)
                RETURN c
            """)
        
        # Create DATA_FLOW_TO relationships from source tables
        for source_table in source_tables:
            queries.append(f"""
                MATCH (src:Table {{
                    user_id: '{user_id}',
                    project_name: '{project_name}',
                    name: '{source_table}'
                }})
                MATCH (tgt:Table {{
                    user_id: '{user_id}',
                    project_name: '{project_name}',
                    schema: '{schema}',
                    name: '{table_name}'
                }})
                MERGE (src)-[r:DATA_FLOW_TO]->(tgt)
                SET r.flow_type = 'ETL_OLAP',
                    r.cube_name = '{cube_name}'
                RETURN src, r, tgt
            """)
        
        # Execute all queries
        results = []
        for query in queries:
            result = await self.execute_query(query)
            results.append(result)
        
        return {
            "success": True,
            "table": table_name,
            "schema": schema,
            "columns_created": len(columns),
            "lineage_relationships": len(source_tables)
        }

    async def register_star_schema(
        self,
        cube_name: str,
        fact_table_name: str,
        fact_columns: List[Dict],
        dimensions: List[Dict],
        dw_schema: str = "dw",
        db_name: str = "meetingroom",
        source_tables: List[str] = None,
        mappings: List[Dict] = None
    ) -> Dict:
        """Register complete star schema in Neo4j with FK relationships.
        
        This creates:
        1. Dimension Table nodes with columns
        2. Fact Table node with columns
        3. FK_TO relationships from fact to dimension tables
        4. FK_TO_TABLE relationships for graph navigation
        5. DERIVED_FROM relationships for data lineage (DW -> Source)
        6. Vector embeddings for semantic search
        
        Args:
            cube_name: Name of the OLAP cube
            fact_table_name: Name of the fact table
            fact_columns: List of {name, dtype, description} for fact measures
            dimensions: List of {name, table_name, columns: [{name, dtype, description}]}
            dw_schema: Schema name (default: dw)
            db_name: Database name (default: meetingroom)
            source_tables: List of source table FQNs (e.g., ["RWIS.RDF01HH_TB"])
            mappings: List of column mappings for lineage
        """
        queries = []
        created_tables = []
        
        # 0. Create Schema node first
        queries.append(f"""
            MERGE (s:Schema {{db: '{db_name}', name: '{dw_schema}'}})
            SET s.description = 'Data Warehouse schema for OLAP cubes',
                s.type = 'DW',
                s.updated_at = datetime()
            RETURN s
        """)
        
        # 1. Create Dimension tables and their columns
        for dim in dimensions:
            dim_table = dim.get("table_name", dim.get("name", "dim_unknown"))
            dim_columns = dim.get("columns", [])
            
            # Add id column
            all_columns = [{"name": "id", "dtype": "SERIAL", "description": "Primary key"}]
            all_columns.extend(dim_columns)
            all_columns.append({"name": "_etl_loaded_at", "dtype": "TIMESTAMP", "description": "ETL load timestamp"})
            
            # Create Table node
            queries.append(f"""
                MERGE (t:Table {{
                    db: '{db_name}',
                    schema: '{dw_schema}',
                    name: '{dim_table}'
                }})
                SET t.table_type = 'DIMENSION',
                    t.cube_name = '{cube_name}',
                    t.description = 'Dimension table for {cube_name}'
                RETURN t
            """)
            
            # Connect Table to Schema via BELONGS_TO
            queries.append(f"""
                MATCH (t:Table {{db: '{db_name}', schema: '{dw_schema}', name: '{dim_table}'}})
                MATCH (s:Schema {{db: '{db_name}', name: '{dw_schema}'}})
                MERGE (t)-[r:BELONGS_TO]->(s)
                RETURN t, r, s
            """)
            
            # Create Column nodes and relationships
            for col in all_columns:
                col_name = col.get("name", "")
                col_dtype = col.get("dtype", "VARCHAR")
                col_desc = col.get("description", "")
                fqn = f"{dw_schema}.{dim_table}.{col_name}".lower()
                
                queries.append(f"""
                    MATCH (t:Table {{
                        db: '{db_name}',
                        schema: '{dw_schema}',
                        name: '{dim_table}'
                    }})
                    MERGE (c:Column {{fqn: '{fqn}'}})
                    SET c.name = '{col_name}',
                        c.dtype = '{col_dtype}',
                        c.description = '{col_desc}'
                    MERGE (t)-[:HAS_COLUMN]->(c)
                    RETURN c
                """)
            
            created_tables.append(dim_table)
        
        # 2. Create Fact table with columns
        fact_all_columns = [{"name": "id", "dtype": "SERIAL", "description": "Primary key"}]
        
        # Add FK columns for each dimension
        for dim in dimensions:
            dim_table = dim.get("table_name", dim.get("name", "dim_unknown"))
            fk_col_name = f"{dim_table}_id"
            fact_all_columns.append({
                "name": fk_col_name,
                "dtype": "INTEGER",
                "description": f"Foreign key to {dim_table}"
            })
        
        # Add measure columns
        for col in fact_columns:
            fact_all_columns.append(col)
        
        fact_all_columns.append({"name": "_etl_loaded_at", "dtype": "TIMESTAMP", "description": "ETL load timestamp"})
        
        # Create Fact Table node
        queries.append(f"""
            MERGE (t:Table {{
                db: '{db_name}',
                schema: '{dw_schema}',
                name: '{fact_table_name}'
            }})
            SET t.table_type = 'FACT',
                t.cube_name = '{cube_name}',
                t.description = 'Fact table for {cube_name}'
            RETURN t
        """)
        
        # Connect Fact Table to Schema via BELONGS_TO
        queries.append(f"""
            MATCH (t:Table {{db: '{db_name}', schema: '{dw_schema}', name: '{fact_table_name}'}})
            MATCH (s:Schema {{db: '{db_name}', name: '{dw_schema}'}})
            MERGE (t)-[r:BELONGS_TO]->(s)
            RETURN t, r, s
        """)
        
        created_tables.append(fact_table_name)
        
        # Create Fact columns
        for col in fact_all_columns:
            col_name = col.get("name", "")
            col_dtype = col.get("dtype", "VARCHAR")
            col_desc = col.get("description", "")
            fqn = f"{dw_schema}.{fact_table_name}.{col_name}".lower()
            
            queries.append(f"""
                MATCH (t:Table {{
                    db: '{db_name}',
                    schema: '{dw_schema}',
                    name: '{fact_table_name}'
                }})
                MERGE (c:Column {{fqn: '{fqn}'}})
                SET c.name = '{col_name}',
                    c.dtype = '{col_dtype}',
                    c.description = '{col_desc}'
                MERGE (t)-[:HAS_COLUMN]->(c)
                RETURN c
            """)
        
        # 3. Create FK relationships (Fact -> Dimensions)
        for dim in dimensions:
            dim_table = dim.get("table_name", dim.get("name", "dim_unknown"))
            fk_col_name = f"{dim_table}_id"
            fk_fqn = f"{dw_schema}.{fact_table_name}.{fk_col_name}".lower()
            pk_fqn = f"{dw_schema}.{dim_table}.id".lower()
            
            # Column-to-column FK_TO relationship
            queries.append(f"""
                MATCH (c1:Column {{fqn: '{fk_fqn}'}})
                MATCH (c2:Column {{fqn: '{pk_fqn}'}})
                MERGE (c1)-[fk:FK_TO]->(c2)
                SET fk.constraint = 'fk_{fact_table_name}_{dim_table}',
                    fk.on_update = 'NO ACTION',
                    fk.on_delete = 'NO ACTION'
                RETURN fk
            """)
            
            # Table-to-table FK_TO_TABLE relationship
            queries.append(f"""
                MATCH (t1:Table {{
                    db: '{db_name}',
                    schema: '{dw_schema}',
                    name: '{fact_table_name}'
                }})
                MATCH (t2:Table {{
                    db: '{db_name}',
                    schema: '{dw_schema}',
                    name: '{dim_table}'
                }})
                MERGE (t1)-[r:FK_TO_TABLE]->(t2)
                SET r.sourceColumn = '{fk_col_name}',
                    r.targetColumn = 'id',
                    r.type = 'FACT_TO_DIM',
                    r.source = 'olap_auto'
                RETURN r
            """)
        
        # 4. Create DERIVED_FROM relationships (DW Tables -> Source Tables)
        lineage_created = 0
        if source_tables:
            for source_fqn in source_tables:
                # Parse source table: "SCHEMA.TABLE" format
                parts = source_fqn.split(".")
                if len(parts) >= 2:
                    source_schema = parts[0].lower()
                    source_table = parts[1].lower()
                    
                    # Create lineage from fact table to source
                    queries.append(f"""
                        MATCH (dw:Table {{
                            db: '{db_name}',
                            schema: '{dw_schema}',
                            name: '{fact_table_name}'
                        }})
                        MATCH (src:Table {{schema: '{source_schema}', name: '{source_table}'}})
                        MERGE (dw)-[r:DERIVED_FROM]->(src)
                        SET r.cube_name = '{cube_name}',
                            r.type = 'ETL',
                            r.created_at = datetime()
                        RETURN r
                    """)
                    lineage_created += 1
                    
                    # Also try to link dimension tables to their sources
                    for dim in dimensions:
                        dim_table = dim.get("table_name", dim.get("name", "dim_unknown"))
                        queries.append(f"""
                            MATCH (dw:Table {{
                                db: '{db_name}',
                                schema: '{dw_schema}',
                                name: '{dim_table}'
                            }})
                            MATCH (src:Table {{schema: '{source_schema}', name: '{source_table}'}})
                            MERGE (dw)-[r:DERIVED_FROM]->(src)
                            SET r.cube_name = '{cube_name}',
                                r.type = 'ETL',
                                r.dimension = '{dim_table}',
                                r.created_at = datetime()
                            RETURN r
                        """)
        
        # 5. Create column-level lineage from mappings
        if mappings:
            for mapping in mappings:
                source_table = mapping.get("source_table", "")
                source_column = mapping.get("source_column", "")
                target_table = mapping.get("target_table", "")
                target_column = mapping.get("target_column", "")
                
                if source_table and source_column and target_table and target_column:
                    # Parse source FQN
                    src_parts = source_table.split(".")
                    src_schema = src_parts[0].lower() if len(src_parts) > 1 else "public"
                    src_tbl = src_parts[-1].lower()
                    
                    # Parse target FQN
                    tgt_parts = target_table.split(".")
                    tgt_schema = tgt_parts[0].lower() if len(tgt_parts) > 1 else dw_schema
                    tgt_tbl = tgt_parts[-1].lower()
                    
                    src_fqn = f"{src_schema}.{src_tbl}.{source_column}".lower()
                    tgt_fqn = f"{tgt_schema}.{tgt_tbl}.{target_column}".lower()
                    
                    queries.append(f"""
                        MATCH (src_col:Column {{fqn: '{src_fqn}'}})
                        MATCH (tgt_col:Column {{fqn: '{tgt_fqn}'}})
                        MERGE (tgt_col)-[r:DERIVED_FROM]->(src_col)
                        SET r.transformation = '{mapping.get("transformation", "DIRECT")}',
                            r.cube_name = '{cube_name}'
                        RETURN r
                    """)
        
        # Execute all queries
        results = []
        for query in queries:
            try:
                result = await self.execute_query(query)
                results.append(result)
            except Exception as e:
                print(f"Warning: Query failed: {str(e)[:100]}")
        
        return {
            "success": True,
            "cube_name": cube_name,
            "schema": dw_schema,
            "schema_node_created": True,
            "tables_created": created_tables,
            "fk_relationships": len(dimensions),
            "belongs_to_relationships": len(created_tables),
            "lineage_relationships": lineage_created
        }

    async def delete_star_schema(
        self,
        cube_name: str = None,
        dw_schema: str = "dw",
        db_name: str = "meetingroom"
    ) -> Dict:
        """Delete star schema tables and relationships from Neo4j.
        
        Args:
            cube_name: Optional cube name to filter by
            dw_schema: Schema name (default: dw)
            db_name: Database name
        """
        if cube_name:
            # Delete specific cube's tables
            query = f"""
                MATCH (t:Table {{db: '{db_name}', schema: '{dw_schema}', cube_name: '{cube_name}'}})
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
                DETACH DELETE t, c
            """
        else:
            # Delete all tables in dw schema
            query = f"""
                MATCH (t:Table {{db: '{db_name}', schema: '{dw_schema}'}})
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
                DETACH DELETE t, c
            """
        
        await self.execute_query(query)
        
        return {
            "success": True,
            "schema": dw_schema,
            "cube_name": cube_name
        }


# Global client instance
neo4j_client = Neo4jClient()

