"""In-memory metadata store for cube definitions."""
from typing import Dict, List, Optional
from ..models.cube import Cube, CubeMetadata


class MetadataStore:
    """Store and retrieve cube metadata."""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern for metadata store."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._cubes: Dict[str, Cube] = {}
            cls._instance._schema_name: Optional[str] = None
        return cls._instance
    
    def load_metadata(self, metadata: CubeMetadata) -> None:
        """Load cube metadata into the store."""
        self._schema_name = metadata.schema_name
        for cube in metadata.cubes:
            self._cubes[cube.name] = cube
    
    def get_cube(self, name: str) -> Optional[Cube]:
        """Get a cube by name."""
        return self._cubes.get(name)
    
    def get_all_cubes(self) -> List[Cube]:
        """Get all loaded cubes."""
        return list(self._cubes.values())
    
    def get_cube_names(self) -> List[str]:
        """Get names of all loaded cubes."""
        return list(self._cubes.keys())
    
    def clear(self) -> None:
        """Clear all stored metadata."""
        self._cubes.clear()
        self._schema_name = None
    
    def get_schema_description(self, cube_name: Optional[str] = None) -> str:
        """Generate a text description of the schema for LLM consumption."""
        cubes = [self._cubes[cube_name]] if cube_name and cube_name in self._cubes else self._cubes.values()
        
        descriptions = []
        for cube in cubes:
            desc = self._describe_cube(cube)
            descriptions.append(desc)
        
        return "\n\n".join(descriptions)
    
    def _describe_cube(self, cube: Cube) -> str:
        """Generate a text description of a single cube."""
        lines = [
            f"## Cube: {cube.name}",
            f"Fact Table: {cube.fact_table}",
            "",
            "### Measures:",
        ]
        
        for measure in cube.measures:
            lines.append(f"  - {measure.name}: {measure.agg}({cube.fact_table}.{measure.column})")
        
        lines.append("")
        lines.append("### Dimensions:")
        
        for dim in cube.dimensions:
            lines.append(f"  - {dim.name} (table: {dim.table})")
            for level in dim.levels:
                lines.append(f"    - Level: {level.name} (column: {level.column})")
        
        if cube.joins:
            lines.append("")
            lines.append("### Joins:")
            for join in cube.joins:
                lines.append(
                    f"  - {join.left_table}.{join.left_key} = {join.right_table}.{join.right_key}"
                )
        
        return "\n".join(lines)


# Global instance
metadata_store = MetadataStore()

