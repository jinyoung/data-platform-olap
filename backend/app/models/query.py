"""Query models for pivot and natural language queries."""
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field


class PivotField(BaseModel):
    """Represents a field in the pivot configuration."""
    dimension: str
    level: str
    
    
class PivotMeasure(BaseModel):
    """Represents a measure in the pivot configuration."""
    name: str
    

class FilterCondition(BaseModel):
    """Represents a filter condition."""
    dimension: str
    level: str
    operator: str = "="  # =, !=, IN, NOT IN, >, <, >=, <=, LIKE
    values: List[Any] = Field(default_factory=list)


class PivotQuery(BaseModel):
    """Pivot query configuration."""
    cube_name: str
    rows: List[PivotField] = Field(default_factory=list)
    columns: List[PivotField] = Field(default_factory=list)
    measures: List[PivotMeasure] = Field(default_factory=list)
    filters: List[FilterCondition] = Field(default_factory=list)
    limit: int = 1000


class NaturalQuery(BaseModel):
    """Natural language query request."""
    question: str
    cube_name: Optional[str] = None


class QueryResult(BaseModel):
    """Query execution result."""
    sql: str
    columns: List[str] = Field(default_factory=list)
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0
    error: Optional[str] = None

