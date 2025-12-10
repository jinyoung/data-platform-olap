"""Data models for AI Pivot Studio."""
from typing import Optional
from pydantic import BaseModel


class Level(BaseModel):
    """Hierarchy level within a dimension."""
    name: str
    column: str
    order_column: Optional[str] = None


class Dimension(BaseModel):
    """Dimension definition for OLAP cube."""
    name: str
    table: str
    foreign_key: Optional[str] = None
    primary_key: Optional[str] = None
    levels: list[Level] = []


class Measure(BaseModel):
    """Measure definition for OLAP cube."""
    name: str
    column: str
    agg: str = "SUM"  # SUM, COUNT, AVG, MIN, MAX


class Cube(BaseModel):
    """OLAP Cube definition."""
    name: str
    fact_table: str
    measures: list[Measure] = []
    dimensions: list[Dimension] = []


class Schema(BaseModel):
    """Complete schema containing multiple cubes."""
    name: str
    cubes: list[Cube] = []


# API Request/Response Models
class PivotConfig(BaseModel):
    """Pivot table configuration."""
    cube_name: str
    rows: list[str] = []  # Dimension level names
    columns: list[str] = []  # Dimension level names
    measures: list[str] = []  # Measure names
    filters: dict[str, list[str]] = {}  # Dimension -> values


class NL2SQLRequest(BaseModel):
    """Natural language to SQL request."""
    query: str
    cube_name: Optional[str] = None


class SQLResult(BaseModel):
    """SQL query result."""
    sql: str
    columns: list[str]
    rows: list[list]
    error: Optional[str] = None


class PivotQueryResponse(BaseModel):
    """Response for pivot query."""
    sql: str
    result: SQLResult


class NL2SQLResponse(BaseModel):
    """Response for natural language to SQL."""
    original_query: str
    sql: str
    result: SQLResult
    explanation: Optional[str] = None

