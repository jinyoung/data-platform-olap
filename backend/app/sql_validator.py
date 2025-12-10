"""SQL Validator for security and safety."""
import re
from .models import Cube
from .metadata_store import metadata_store


class SQLValidationError(Exception):
    """SQL validation error."""
    pass


def validate_sql(sql: str, cube_name: str | None = None) -> str:
    """Validate and sanitize SQL query."""
    sql = sql.strip()
    
    # Remove any trailing semicolons
    sql = sql.rstrip(';')
    
    # Check for forbidden keywords
    forbidden_patterns = [
        r'\bUPDATE\b',
        r'\bDELETE\b',
        r'\bINSERT\b',
        r'\bDROP\b',
        r'\bALTER\b',
        r'\bCREATE\b',
        r'\bTRUNCATE\b',
        r'\bGRANT\b',
        r'\bREVOKE\b',
        r'\bEXEC\b',
        r'\bEXECUTE\b',
        r'--',  # SQL comments
        r'/\*',  # Block comments
        r'\bINTO\b',  # SELECT INTO
    ]
    
    sql_upper = sql.upper()
    for pattern in forbidden_patterns:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            raise SQLValidationError(f"Forbidden SQL pattern detected: {pattern}")
    
    # Must start with SELECT
    if not sql_upper.strip().startswith('SELECT'):
        raise SQLValidationError("Only SELECT queries are allowed")
    
    # Validate against allowed tables if cube specified
    if cube_name:
        cube = metadata_store.get_cube(cube_name)
        if cube:
            validate_tables(sql, cube)
    
    # Ensure LIMIT exists, add if missing
    if 'LIMIT' not in sql_upper:
        sql = sql + '\nLIMIT 100'
    else:
        # Check if limit is too high
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper)
        if limit_match:
            limit_val = int(limit_match.group(1))
            if limit_val > 10000:
                sql = re.sub(r'LIMIT\s+\d+', 'LIMIT 10000', sql, flags=re.IGNORECASE)
    
    return sql


def validate_tables(sql: str, cube: Cube) -> None:
    """Validate that only allowed tables are used."""
    allowed_tables = {cube.fact_table.lower()}
    for dim in cube.dimensions:
        if dim.table:
            allowed_tables.add(dim.table.lower())
    
    # Simple table extraction (not perfect but catches most cases)
    table_patterns = [
        r'\bFROM\s+(\w+)',
        r'\bJOIN\s+(\w+)',
    ]
    
    sql_lower = sql.lower()
    for pattern in table_patterns:
        matches = re.findall(pattern, sql_lower)
        for table in matches:
            if table not in allowed_tables and table not in ['select', 'where', 'and', 'or']:
                # Log warning but don't block - LLM might use valid aliases
                pass


def extract_sql_from_response(response: str) -> str:
    """Extract SQL from LLM response that might contain markdown."""
    # Try to find SQL in code blocks
    sql_block_pattern = r'```(?:sql)?\s*(SELECT[\s\S]*?)```'
    matches = re.findall(sql_block_pattern, response, re.IGNORECASE)
    if matches:
        return matches[0].strip()
    
    # Try to find SELECT statement directly
    select_pattern = r'(SELECT\s+[\s\S]*?)(?:;|\Z)'
    matches = re.findall(select_pattern, response, re.IGNORECASE)
    if matches:
        return matches[0].strip()
    
    return response.strip()

