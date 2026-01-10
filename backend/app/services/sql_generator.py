"""SQL Generator for pivot queries."""
from typing import List, Set
from ..models.cube import Cube
from ..models.query import PivotQuery, FilterCondition


class SQLGenerator:
    """Generate SQL queries from pivot configurations."""
    
    DW_SCHEMA = "dw"  # Default OLAP schema
    
    def __init__(self, cube: Cube):
        self.cube = cube
    
    def _full_table_name(self, table: str) -> str:
        """Get full table name with schema prefix and proper quoting."""
        if '.' in table:
            # Split schema.table and quote table name if needed
            parts = table.split('.', 1)
            schema = parts[0]
            table_name = parts[1]
            quoted_table = self._quote_identifier(table_name)
            return f"{schema}.{quoted_table}"
        # No schema, quote if needed and add default schema
        quoted_table = self._quote_identifier(table)
        return f"{self.DW_SCHEMA}.{quoted_table}"
    
    def _quote_identifier(self, name: str) -> str:
        """Quote an identifier if it contains special characters or spaces."""
        # Check if quoting is needed: non-ASCII, spaces, or special chars
        needs_quoting = (
            ' ' in name or 
            not name.isascii() or 
            not name.replace('_', '').isalnum()
        )
        if needs_quoting:
            # Escape any existing double quotes
            escaped = name.replace('"', '""')
            return f'"{escaped}"'
        return name
    
    def _safe_alias(self, name: str) -> str:
        """Create a safe SQL alias from a name."""
        # For aliases, convert to snake_case if it has Korean or spaces
        if ' ' in name or not name.isascii():
            # Use quoted identifier
            return self._quote_identifier(name)
        return name
    
    def generate_pivot_sql(self, query: PivotQuery) -> str:
        """Generate SQL for a pivot query."""
        # Collect required tables and columns
        select_parts = []
        group_by_parts = []
        tables: Set[str] = {self.cube.fact_table}
        joins = []
        
        fact_table = self._full_table_name(self.cube.fact_table)
        
        # Process row dimensions
        for row_field in query.rows:
            dim = self._get_dimension(row_field.dimension)
            if dim:
                level = self._get_level(dim, row_field.level)
                if level:
                    dim_table = self._full_table_name(dim.table)
                    col_ref = f"{dim_table}.{level.column}"
                    alias = f"{row_field.dimension}_{row_field.level}"
                    select_parts.append(f"{col_ref} AS {alias}")
                    group_by_parts.append(col_ref)
                    
                    if dim.table != self.cube.fact_table:
                        tables.add(dim.table)
                        join = self._get_join(dim.table)
                        if join:
                            joins.append(join)
        
        # Process column dimensions
        for col_field in query.columns:
            dim = self._get_dimension(col_field.dimension)
            if dim:
                level = self._get_level(dim, col_field.level)
                if level:
                    dim_table = self._full_table_name(dim.table)
                    col_ref = f"{dim_table}.{level.column}"
                    alias = f"{col_field.dimension}_{col_field.level}"
                    select_parts.append(f"{col_ref} AS {alias}")
                    group_by_parts.append(col_ref)
                    
                    if dim.table != self.cube.fact_table:
                        tables.add(dim.table)
                        join = self._get_join(dim.table)
                        if join:
                            joins.append(join)
        
        # Process measures
        for measure_field in query.measures:
            measure = self._get_measure(measure_field.name)
            if measure:
                if measure.agg == "COUNT DISTINCT":
                    agg_expr = f"COUNT(DISTINCT {fact_table}.{measure.column})"
                else:
                    agg_expr = f"{measure.agg}({fact_table}.{measure.column})"
                # Use safe alias for measure names (may contain Korean or spaces)
                safe_alias = self._safe_alias(measure.name)
                select_parts.append(f"{agg_expr} AS {safe_alias}")
        
        # Build SQL
        if not select_parts:
            return "SELECT 1"  # Empty query fallback
        
        sql = f"SELECT {', '.join(select_parts)}"
        sql += f"\nFROM {fact_table}"
        
        # Add joins
        seen_tables = {self.cube.fact_table}
        for join in joins:
            if join[0] not in seen_tables:
                join_table = self._full_table_name(join[0])
                sql += f"\nJOIN {join_table} ON {join[1]}"
                seen_tables.add(join[0])
        
        # Add filters
        where_clauses = self._build_where_clauses(query.filters)
        if where_clauses:
            sql += f"\nWHERE {' AND '.join(where_clauses)}"
        
        # Add GROUP BY
        if group_by_parts:
            sql += f"\nGROUP BY {', '.join(group_by_parts)}"
        
        # Add ORDER BY (same as GROUP BY for consistency)
        if group_by_parts:
            sql += f"\nORDER BY {', '.join(group_by_parts)}"
        
        # Add LIMIT
        sql += f"\nLIMIT {query.limit}"
        
        return sql
    
    def _get_dimension(self, name: str):
        """Find a dimension by name."""
        for dim in self.cube.dimensions:
            if dim.name == name:
                return dim
        return None
    
    def _get_level(self, dim, name: str):
        """Find a level in a dimension by name."""
        for level in dim.levels:
            if level.name == name:
                return level
        return None
    
    def _get_measure(self, name: str):
        """Find a measure by name."""
        for measure in self.cube.measures:
            if measure.name == name:
                return measure
        return None
    
    def _get_join(self, table: str) -> tuple:
        """Get join clause for a table."""
        fact_table = self._full_table_name(self.cube.fact_table)
        dim_table = self._full_table_name(table)
        
        for join in self.cube.joins:
            if join.right_table == table:
                left_tbl = self._full_table_name(join.left_table)
                right_tbl = self._full_table_name(join.right_table)
                return (
                    table,
                    f"{left_tbl}.{join.left_key} = {right_tbl}.{join.right_key}"
                )
        # Fallback: try to find dimension with foreign key
        for dim in self.cube.dimensions:
            if dim.table == table and dim.foreign_key:
                # Assume primary key is 'id' if not specified
                return (
                    table,
                    f"{fact_table}.{dim.foreign_key} = {dim_table}.id"
                )
        return None
    
    def _build_where_clauses(self, filters: List[FilterCondition]) -> List[str]:
        """Build WHERE clause parts from filters."""
        clauses = []
        for f in filters:
            dim = self._get_dimension(f.dimension)
            if dim:
                level = self._get_level(dim, f.level)
                if level:
                    dim_table = self._full_table_name(dim.table)
                    col_ref = f"{dim_table}.{level.column}"
                    clause = self._format_filter(col_ref, f.operator, f.values)
                    if clause:
                        clauses.append(clause)
        return clauses
    
    def _format_filter(self, column: str, operator: str, values: List) -> str:
        """Format a single filter condition."""
        if not values:
            return ""
        
        if operator.upper() == "IN":
            vals = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
            return f"{column} IN ({vals})"
        elif operator.upper() == "NOT IN":
            vals = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
            return f"{column} NOT IN ({vals})"
        elif operator.upper() == "LIKE":
            return f"{column} LIKE '{values[0]}'"
        else:
            val = values[0]
            if isinstance(val, str):
                return f"{column} {operator} '{val}'"
            return f"{column} {operator} {val}"

