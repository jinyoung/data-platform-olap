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


# Global client instance
neo4j_client = Neo4jClient()

