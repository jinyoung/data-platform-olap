"""Mondrian XML Schema Parser."""
from typing import Optional
from lxml import etree
from ..models.cube import (
    Cube, Measure, Dimension, Level, Join, CubeMetadata
)


class MondrianXMLParser:
    """Parse Mondrian XML schema files into internal metadata structures."""
    
    def parse(self, xml_content: str) -> CubeMetadata:
        """Parse XML content and return CubeMetadata."""
        root = etree.fromstring(xml_content.encode('utf-8'))
        
        # Handle namespace if present
        nsmap = root.nsmap
        ns = nsmap.get(None, '')
        
        schema_name = root.get('name', 'Default')
        cubes = []
        
        # Find all Cube elements
        cube_elements = root.findall('.//Cube') or root.findall('.//{%s}Cube' % ns if ns else './/Cube')
        
        for cube_elem in cube_elements:
            cube = self._parse_cube(cube_elem)
            if cube:
                cubes.append(cube)
        
        return CubeMetadata(cubes=cubes, schema_name=schema_name)
    
    def _parse_cube(self, cube_elem) -> Optional[Cube]:
        """Parse a single Cube element."""
        name = cube_elem.get('name')
        if not name:
            return None
        
        # Get fact table
        table_elem = cube_elem.find('Table')
        if table_elem is not None:
            fact_table = table_elem.get('name', '')
        else:
            fact_table = cube_elem.get('fact_table', name.lower())
        
        # Parse measures
        measures = []
        for measure_elem in cube_elem.findall('.//Measure'):
            measure = self._parse_measure(measure_elem)
            if measure:
                measures.append(measure)
        
        # Parse dimensions
        dimensions = []
        joins = []
        for dim_elem in cube_elem.findall('.//Dimension'):
            dim, dim_joins = self._parse_dimension(dim_elem, fact_table)
            if dim:
                dimensions.append(dim)
                joins.extend(dim_joins)
        
        # Also check for DimensionUsage (shared dimensions)
        for dim_usage in cube_elem.findall('.//DimensionUsage'):
            dim = self._parse_dimension_usage(dim_usage)
            if dim:
                dimensions.append(dim)
        
        return Cube(
            name=name,
            fact_table=fact_table,
            measures=measures,
            dimensions=dimensions,
            joins=joins,
            caption=cube_elem.get('caption')
        )
    
    def _parse_measure(self, measure_elem) -> Optional[Measure]:
        """Parse a Measure element."""
        name = measure_elem.get('name')
        column = measure_elem.get('column')
        
        if not name or not column:
            return None
        
        agg = measure_elem.get('aggregator', 'sum').upper()
        # Map Mondrian aggregators to SQL
        agg_map = {
            'SUM': 'SUM',
            'COUNT': 'COUNT',
            'AVG': 'AVG',
            'MIN': 'MIN',
            'MAX': 'MAX',
            'DISTINCT-COUNT': 'COUNT DISTINCT'
        }
        agg = agg_map.get(agg, 'SUM')
        
        return Measure(
            name=name,
            column=column,
            agg=agg,
            format_string=measure_elem.get('formatString'),
            caption=measure_elem.get('caption')
        )
    
    def _parse_dimension(self, dim_elem, fact_table: str) -> tuple:
        """Parse a Dimension element."""
        name = dim_elem.get('name')
        if not name:
            return None, []
        
        # Get dimension table
        table_elem = dim_elem.find('.//Table')
        if table_elem is not None:
            table = table_elem.get('name', '')
        else:
            table = dim_elem.get('table', name.lower())
        
        foreign_key = dim_elem.get('foreignKey')
        
        # Parse hierarchy and levels
        levels = []
        joins = []
        
        hierarchy_elem = dim_elem.find('.//Hierarchy')
        if hierarchy_elem is not None:
            # Check for hierarchy table
            hier_table_elem = hierarchy_elem.find('Table')
            if hier_table_elem is not None:
                table = hier_table_elem.get('name', table)
            
            primary_key = hierarchy_elem.get('primaryKey')
            
            # Create join if we have foreign key and primary key
            if foreign_key and primary_key and table != fact_table:
                joins.append(Join(
                    left_table=fact_table,
                    left_key=foreign_key,
                    right_table=table,
                    right_key=primary_key
                ))
            
            for level_elem in hierarchy_elem.findall('Level'):
                level = self._parse_level(level_elem)
                if level:
                    levels.append(level)
        else:
            # Direct levels under dimension
            for level_elem in dim_elem.findall('Level'):
                level = self._parse_level(level_elem)
                if level:
                    levels.append(level)
        
        return Dimension(
            name=name,
            table=table,
            foreign_key=foreign_key,
            levels=levels,
            caption=dim_elem.get('caption')
        ), joins
    
    def _parse_level(self, level_elem) -> Optional[Level]:
        """Parse a Level element."""
        name = level_elem.get('name')
        column = level_elem.get('column')
        
        if not name:
            return None
        
        # Use name as column if column not specified
        if not column:
            column = name.lower().replace(' ', '_')
        
        return Level(
            name=name,
            column=column,
            order_column=level_elem.get('ordinalColumn'),
            caption=level_elem.get('caption')
        )
    
    def _parse_dimension_usage(self, dim_usage) -> Optional[Dimension]:
        """Parse a DimensionUsage element (shared dimension reference)."""
        name = dim_usage.get('name')
        source = dim_usage.get('source', name)
        foreign_key = dim_usage.get('foreignKey')
        
        if not name:
            return None
        
        # Create a placeholder dimension
        # In a full implementation, this would reference a shared dimension definition
        return Dimension(
            name=name,
            table=source.lower() if source else name.lower(),
            foreign_key=foreign_key,
            levels=[Level(name=name, column=name.lower())],
            caption=dim_usage.get('caption')
        )

