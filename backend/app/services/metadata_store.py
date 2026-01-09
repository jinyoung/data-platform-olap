"""Metadata store for cube definitions with file persistence."""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional
from ..models.cube import Cube, CubeMetadata


# Storage directory
STORAGE_DIR = Path(__file__).parent.parent.parent / "data"
CUBES_FILE = STORAGE_DIR / "cubes.json"


class MetadataStore:
    """Store and retrieve cube metadata with file persistence."""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern for metadata store."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._cubes: Dict[str, Cube] = {}
            cls._instance._schema_name: Optional[str] = None
            cls._instance._initialized = False
        return cls._instance
    
    def _ensure_initialized(self) -> None:
        """Load persisted cubes on first access."""
        if not self._initialized:
            self._load_from_file()
            self._initialized = True
    
    def _load_from_file(self) -> None:
        """Load cubes from JSON file."""
        if CUBES_FILE.exists():
            try:
                with open(CUBES_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for name, cube_dict in data.get('cubes', {}).items():
                        self._cubes[name] = Cube(**cube_dict)
                    self._schema_name = data.get('schema_name')
                print(f"Loaded {len(self._cubes)} cubes from {CUBES_FILE}")
            except Exception as e:
                print(f"Failed to load cubes from file: {e}")
    
    def _save_to_file(self) -> None:
        """Save cubes to JSON file."""
        try:
            STORAGE_DIR.mkdir(parents=True, exist_ok=True)
            data = {
                'schema_name': self._schema_name,
                'cubes': {name: cube.model_dump() for name, cube in self._cubes.items()}
            }
            with open(CUBES_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(self._cubes)} cubes to {CUBES_FILE}")
        except Exception as e:
            print(f"Failed to save cubes to file: {e}")
    
    def load_metadata(self, metadata: CubeMetadata) -> None:
        """Load cube metadata into the store and persist."""
        self._ensure_initialized()
        self._schema_name = metadata.schema_name
        for cube in metadata.cubes:
            self._cubes[cube.name] = cube
        self._save_to_file()
    
    def get_cube(self, name: str) -> Optional[Cube]:
        """Get a cube by name."""
        self._ensure_initialized()
        return self._cubes.get(name)
    
    def get_all_cubes(self) -> List[Cube]:
        """Get all loaded cubes."""
        self._ensure_initialized()
        return list(self._cubes.values())
    
    def get_cube_names(self) -> List[str]:
        """Get names of all loaded cubes."""
        self._ensure_initialized()
        return list(self._cubes.keys())
    
    def clear(self) -> None:
        """Clear all stored metadata."""
        self._cubes.clear()
        self._schema_name = None
        self._save_to_file()
    
    def delete_cube(self, name: str) -> bool:
        """Delete a cube by name."""
        self._ensure_initialized()
        if name in self._cubes:
            del self._cubes[name]
            self._save_to_file()
            return True
        return False
    
    def get_schema_description(self, cube_name: Optional[str] = None) -> str:
        """Generate a text description of the schema for LLM consumption."""
        self._ensure_initialized()
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
