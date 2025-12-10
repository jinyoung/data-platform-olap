"""Database executor for running SQL queries."""
import asyncio
import time
from typing import List, Dict, Any, Optional
import asyncpg
from ..core.config import get_settings
from ..models.query import QueryResult


class DatabaseExecutor:
    """Execute SQL queries against PostgreSQL."""
    
    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or get_settings().database_url
        self._pool: Optional[asyncpg.Pool] = None
    
    async def get_pool(self) -> asyncpg.Pool:
        """Get or create connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=1,
                max_size=10,
                command_timeout=get_settings().query_timeout
            )
        return self._pool
    
    async def execute_query(self, sql: str) -> QueryResult:
        """Execute a SQL query and return results."""
        start_time = time.time()
        
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                # Execute query
                rows = await conn.fetch(sql)
                
                # Convert to list of dicts
                columns = list(rows[0].keys()) if rows else []
                data = [dict(row) for row in rows]
                
                execution_time = (time.time() - start_time) * 1000
                
                return QueryResult(
                    sql=sql,
                    columns=columns,
                    rows=data,
                    row_count=len(data),
                    execution_time_ms=round(execution_time, 2)
                )
        except asyncpg.PostgresError as e:
            return QueryResult(
                sql=sql,
                error=f"Database error: {str(e)}"
            )
        except asyncio.TimeoutError:
            return QueryResult(
                sql=sql,
                error="Query timeout exceeded"
            )
        except Exception as e:
            return QueryResult(
                sql=sql,
                error=f"Execution error: {str(e)}"
            )
    
    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None


# Global executor instance
db_executor = DatabaseExecutor()

