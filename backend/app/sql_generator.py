"""SQL Generator for Pivot Queries."""
from .models import Cube, PivotConfig
from .metadata_store import metadata_store


def generate_pivot_sql(config: PivotConfig) -> str:
    """Generate SQL from pivot configuration."""
    cube = metadata_store.get_cube(config.cube_name)
    if not cube:
        raise ValueError(f"Cube '{config.cube_name}' not found")
    
    # Build SELECT clause
    select_parts = []
    group_by_parts = []
    
    # Add row dimensions
    for row_level in config.rows:
        col_expr = find_level_column(cube, row_level)
        if col_expr:
            select_parts.append(f"{col_expr} AS {sanitize_alias(row_level)}")
            group_by_parts.append(col_expr)
    
    # Add column dimensions
    for col_level in config.columns:
        col_expr = find_level_column(cube, col_level)
        if col_expr:
            select_parts.append(f"{col_expr} AS {sanitize_alias(col_level)}")
            group_by_parts.append(col_expr)
    
    # Add measures
    for measure_name in config.measures:
        measure = find_measure(cube, measure_name)
        if measure:
            agg = measure.agg
            col = f"{cube.fact_table}.{measure.column}"
            if agg == "COUNT DISTINCT":
                select_parts.append(f"COUNT(DISTINCT {col}) AS {sanitize_alias(measure_name)}")
            else:
                select_parts.append(f"{agg}({col}) AS {sanitize_alias(measure_name)}")
    
    if not select_parts:
        select_parts = ["COUNT(*) AS row_count"]
    
    # Build FROM clause with JOINs
    from_clause = cube.fact_table
    join_clauses = []
    
    used_tables = set()
    for dim in cube.dimensions:
        if needs_dimension(dim, config):
            if dim.table and dim.table not in used_tables and dim.table != cube.fact_table:
                join_type = "LEFT JOIN"
                fk = dim.foreign_key or f"{dim.name.lower()}_id"
                pk = dim.primary_key or "id"
                join_clauses.append(
                    f"{join_type} {dim.table} ON {cube.fact_table}.{fk} = {dim.table}.{pk}"
                )
                used_tables.add(dim.table)
    
    if join_clauses:
        from_clause += "\n" + "\n".join(join_clauses)
    
    # Build WHERE clause
    where_parts = []
    for dim_name, values in config.filters.items():
        if values:
            col_expr = find_dimension_filter_column(cube, dim_name)
            if col_expr:
                escaped_values = [f"'{v}'" for v in values]
                where_parts.append(f"{col_expr} IN ({', '.join(escaped_values)})")
    
    # Assemble SQL
    sql = f"SELECT\n  {',\n  '.join(select_parts)}\nFROM {from_clause}"
    
    if where_parts:
        sql += f"\nWHERE {' AND '.join(where_parts)}"
    
    if group_by_parts:
        sql += f"\nGROUP BY {', '.join(group_by_parts)}"
    
    # Add ORDER BY for row dimensions
    if config.rows:
        order_cols = [sanitize_alias(r) for r in config.rows]
        sql += f"\nORDER BY {', '.join(order_cols)}"
    
    sql += "\nLIMIT 1000"
    
    return sql


def find_level_column(cube: Cube, level_name: str) -> str | None:
    """Find the column expression for a level."""
    for dim in cube.dimensions:
        for level in dim.levels:
            if level.name == level_name:
                return f"{dim.table}.{level.column}"
    return None


def find_measure(cube: Cube, measure_name: str):
    """Find a measure by name."""
    for m in cube.measures:
        if m.name == measure_name:
            return m
    return None


def find_dimension_filter_column(cube: Cube, dim_name: str) -> str | None:
    """Find column for dimension filter."""
    for dim in cube.dimensions:
        if dim.name == dim_name:
            if dim.levels:
                return f"{dim.table}.{dim.levels[0].column}"
    return None


def needs_dimension(dim, config: PivotConfig) -> bool:
    """Check if dimension is needed for the query."""
    for level in dim.levels:
        if level.name in config.rows or level.name in config.columns:
            return True
    if dim.name in config.filters:
        return True
    return False


def sanitize_alias(name: str) -> str:
    """Sanitize column alias."""
    return name.replace(" ", "_").replace("-", "_").lower()

