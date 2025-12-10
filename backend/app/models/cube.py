"""Cube data models for Mondrian schema representation."""
from typing import List, Optional
from pydantic import BaseModel, Field


class Level(BaseModel):
    """Represents a hierarchy level in a dimension."""
    name: str
    column: str
    order_column: Optional[str] = None
    caption: Optional[str] = None


class Dimension(BaseModel):
    """Represents a dimension in the cube."""
    name: str
    table: str
    foreign_key: Optional[str] = None
    levels: List[Level] = Field(default_factory=list)
    caption: Optional[str] = None


class Measure(BaseModel):
    """Represents a measure in the cube."""
    name: str
    column: str
    agg: str = "SUM"  # SUM, COUNT, AVG, MIN, MAX
    format_string: Optional[str] = None
    caption: Optional[str] = None


class Join(BaseModel):
    """Represents a join between tables."""
    left_table: str
    left_key: str
    right_table: str
    right_key: str


class Cube(BaseModel):
    """Represents a cube definition."""
    name: str
    fact_table: str
    measures: List[Measure] = Field(default_factory=list)
    dimensions: List[Dimension] = Field(default_factory=list)
    joins: List[Join] = Field(default_factory=list)
    caption: Optional[str] = None


class CubeMetadata(BaseModel):
    """Collection of cubes from a schema."""
    cubes: List[Cube] = Field(default_factory=list)
    schema_name: Optional[str] = None

