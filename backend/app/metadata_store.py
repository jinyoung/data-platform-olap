"""In-memory metadata store for cubes."""
from typing import Optional
from .models import Schema, Cube


class MetadataStore:
    """In-memory store for schema metadata."""
    
    def __init__(self):
        self._schemas: dict[str, Schema] = {}
        self._cubes: dict[str, Cube] = {}
    
    def add_schema(self, schema: Schema) -> None:
        """Add a schema to the store."""
        self._schemas[schema.name] = schema
        for cube in schema.cubes:
            self._cubes[cube.name] = cube
    
    def get_schema(self, name: str) -> Optional[Schema]:
        """Get schema by name."""
        return self._schemas.get(name)
    
    def get_cube(self, name: str) -> Optional[Cube]:
        """Get cube by name."""
        return self._cubes.get(name)
    
    def list_cubes(self) -> list[str]:
        """List all cube names."""
        return list(self._cubes.keys())
    
    def get_all_cubes(self) -> list[Cube]:
        """Get all cubes."""
        return list(self._cubes.values())
    
    def clear(self) -> None:
        """Clear all stored data."""
        self._schemas.clear()
        self._cubes.clear()
    
    def get_cube_metadata_text(self, cube_name: str) -> Optional[str]:
        """Get cube metadata as text description for LLM."""
        cube = self.get_cube(cube_name)
        if not cube:
            return None
        
        lines = [
            f"Cube: {cube.name}",
            f"Fact Table: {cube.fact_table}",
            "",
            "Measures:",
        ]
        
        for m in cube.measures:
            lines.append(f"  - {m.name}: {m.agg}({cube.fact_table}.{m.column})")
        
        lines.append("")
        lines.append("Dimensions:")
        
        for d in cube.dimensions:
            lines.append(f"  - {d.name} (table: {d.table})")
            if d.foreign_key:
                lines.append(f"    JOIN: {cube.fact_table}.{d.foreign_key} = {d.table}.{d.primary_key or 'id'}")
            for level in d.levels:
                lines.append(f"    Level: {level.name} (column: {d.table}.{level.column})")
        
        return "\n".join(lines)
    
    def get_all_metadata_text(self) -> str:
        """Get all cubes metadata as text."""
        texts = []
        for cube_name in self.list_cubes():
            text = self.get_cube_metadata_text(cube_name)
            if text:
                texts.append(text)
        return "\n\n---\n\n".join(texts)


# Global store instance
metadata_store = MetadataStore()

