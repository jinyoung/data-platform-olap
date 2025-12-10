"""SQL Generator for pivot queries."""
from typing import List, Set
from ..models.cube import Cube
from ..models.query import PivotQuery, FilterCondition


class SQLGenerator:
    """Generate SQL queries from pivot configurations."""
    
    def __init__(self, cube: Cube):
        self.cube = cube
    
    def generate_pivot_sql(self, query: PivotQuery) -> str:
        """Generate SQL for a pivot query."""
        # Collect required tables and columns
        select_parts = []
        group_by_parts = []
        tables: Set[str] = {self.cube.fact_table}
        joins = []
        
        # Process row dimensions
        for row_field in query.rows:
            dim = self._get_dimension(row_field.dimension)
            if dim:
                level = self._get_level(dim, row_field.level)
                if level:
                    col_ref = f"{dim.table}.{level.column}"
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
                    col_ref = f"{dim.table}.{level.column}"
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
                    agg_expr = f"COUNT(DISTINCT {self.cube.fact_table}.{measure.column})"
                else:
                    agg_expr = f"{measure.agg}({self.cube.fact_table}.{measure.column})"
                select_parts.append(f"{agg_expr} AS {measure.name}")
        
        # Build SQL
        if not select_parts:
            return "SELECT 1"  # Empty query fallback
        
        sql = f"SELECT {', '.join(select_parts)}"
        sql += f"\nFROM {self.cube.fact_table}"
        
        # Add joins
        seen_tables = {self.cube.fact_table}
        for join in joins:
            if join[0] not in seen_tables:
                sql += f"\nJOIN {join[0]} ON {join[1]}"
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
        for join in self.cube.joins:
            if join.right_table == table:
                return (
                    table,
                    f"{join.left_table}.{join.left_key} = {join.right_table}.{join.right_key}"
                )
        # Fallback: try to find dimension with foreign key
        for dim in self.cube.dimensions:
            if dim.table == table and dim.foreign_key:
                # Assume primary key is 'id' if not specified
                return (
                    table,
                    f"{self.cube.fact_table}.{dim.foreign_key} = {table}.id"
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
                    col_ref = f"{dim.table}.{level.column}"
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

