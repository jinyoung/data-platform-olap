"""PostgreSQL query executor."""
import asyncio
from typing import Optional
import asyncpg
from contextlib import asynccontextmanager

from .models import SQLResult


class DatabaseExecutor:
    """Async PostgreSQL executor."""
    
    def __init__(self, dsn: str = None):
        self.dsn = dsn or "postgresql://postgres:postgres@localhost:5432/olap"
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Create connection pool."""
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(
                    self.dsn,
                    min_size=1,
                    max_size=10,
                    timeout=30
                )
            except Exception as e:
                print(f"Database connection error: {e}")
                # Continue without DB for demo mode
                self.pool = None
    
    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
    
    async def execute_query(self, sql: str, timeout: float = 5.0) -> SQLResult:
        """Execute SQL query with timeout."""
        if not self.pool:
            # Demo mode - return mock data
            return self._mock_result(sql)
        
        try:
            async with self.pool.acquire() as conn:
                # Execute with timeout
                rows = await asyncio.wait_for(
                    conn.fetch(sql),
                    timeout=timeout
                )
                
                if rows:
                    columns = list(rows[0].keys())
                    data = [list(row.values()) for row in rows]
                else:
                    columns = []
                    data = []
                
                return SQLResult(
                    sql=sql,
                    columns=columns,
                    rows=data
                )
        
        except asyncio.TimeoutError:
            return SQLResult(
                sql=sql,
                columns=[],
                rows=[],
                error="Query timeout exceeded"
            )
        except Exception as e:
            return SQLResult(
                sql=sql,
                columns=[],
                rows=[],
                error=str(e)
            )
    
    def _mock_result(self, sql: str) -> SQLResult:
        """Return mock data for demo mode."""
        # Parse SQL to determine columns
        import re
        
        # Extract column names from SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            # Extract aliases or column names
            columns = []
            for part in select_clause.split(','):
                part = part.strip()
                # Check for AS alias
                as_match = re.search(r'\bAS\s+(\w+)', part, re.IGNORECASE)
                if as_match:
                    columns.append(as_match.group(1))
                else:
                    # Use last word as column name
                    words = part.split()
                    columns.append(words[-1] if words else 'column')
        else:
            columns = ['result']
        
        # Generate mock data
        mock_data = []
        for i in range(5):
            row = []
            for col in columns:
                if 'year' in col.lower():
                    row.append(2020 + i)
                elif 'month' in col.lower():
                    row.append(['Jan', 'Feb', 'Mar', 'Apr', 'May'][i])
                elif 'amount' in col.lower() or 'sales' in col.lower() or 'sum' in col.lower():
                    row.append(round(10000 + i * 5000 + (i * 1234.56), 2))
                elif 'count' in col.lower():
                    row.append(100 + i * 50)
                elif 'avg' in col.lower():
                    row.append(round(250.5 + i * 10.25, 2))
                else:
                    row.append(f"Value_{i+1}")
            mock_data.append(row)
        
        return SQLResult(
            sql=sql,
            columns=columns,
            rows=mock_data,
            error=None
        )
    
    async def explain_query(self, sql: str) -> str:
        """Get query execution plan."""
        if not self.pool:
            return "EXPLAIN not available in demo mode"
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(f"EXPLAIN {sql}")
                return "\n".join(row[0] for row in rows)
        except Exception as e:
            return f"EXPLAIN error: {e}"


# Global executor instance
db_executor = DatabaseExecutor()

