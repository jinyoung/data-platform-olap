"""Mondrian XML Schema Parser."""
from lxml import etree
from .models import Schema, Cube, Dimension, Measure, Level


def parse_mondrian_xml(xml_content: str | bytes) -> Schema:
    """Parse Mondrian XML schema into internal model."""
    if isinstance(xml_content, str):
        xml_content = xml_content.encode('utf-8')
    
    root = etree.fromstring(xml_content)
    schema_name = root.get('name', 'DefaultSchema')
    
    cubes = []
    for cube_elem in root.findall('.//Cube'):
        cube = parse_cube(cube_elem)
        cubes.append(cube)
    
    return Schema(name=schema_name, cubes=cubes)


def parse_cube(cube_elem) -> Cube:
    """Parse a Cube element."""
    cube_name = cube_elem.get('name', 'UnnamedCube')
    
    # Get fact table
    table_elem = cube_elem.find('Table')
    fact_table = table_elem.get('name') if table_elem is not None else 'unknown_table'
    
    # Parse dimensions
    dimensions = []
    for dim_elem in cube_elem.findall('Dimension'):
        dim = parse_dimension(dim_elem)
        dimensions.append(dim)
    
    # Parse dimension usages (references to shared dimensions)
    for dim_usage in cube_elem.findall('DimensionUsage'):
        dim = parse_dimension_usage(dim_usage)
        dimensions.append(dim)
    
    # Parse measures
    measures = []
    for measure_elem in cube_elem.findall('Measure'):
        measure = parse_measure(measure_elem)
        measures.append(measure)
    
    return Cube(
        name=cube_name,
        fact_table=fact_table,
        measures=measures,
        dimensions=dimensions
    )


def parse_dimension(dim_elem) -> Dimension:
    """Parse a Dimension element."""
    dim_name = dim_elem.get('name', 'UnnamedDimension')
    foreign_key = dim_elem.get('foreignKey')
    
    # Get table from hierarchy or dimension level
    hierarchy = dim_elem.find('Hierarchy')
    table = None
    primary_key = None
    
    if hierarchy is not None:
        table_elem = hierarchy.find('Table')
        if table_elem is not None:
            table = table_elem.get('name')
        primary_key = hierarchy.get('primaryKey')
    
    # Parse levels
    levels = []
    level_elems = hierarchy.findall('Level') if hierarchy is not None else dim_elem.findall('Level')
    for level_elem in level_elems:
        level = parse_level(level_elem)
        levels.append(level)
    
    return Dimension(
        name=dim_name,
        table=table or 'unknown_table',
        foreign_key=foreign_key,
        primary_key=primary_key,
        levels=levels
    )


def parse_dimension_usage(dim_usage) -> Dimension:
    """Parse a DimensionUsage element (reference to shared dimension)."""
    return Dimension(
        name=dim_usage.get('name', 'UnnamedDimension'),
        table=dim_usage.get('source', 'unknown_table'),
        foreign_key=dim_usage.get('foreignKey'),
        levels=[]
    )


def parse_level(level_elem) -> Level:
    """Parse a Level element."""
    return Level(
        name=level_elem.get('name', 'UnnamedLevel'),
        column=level_elem.get('column', 'unknown_column'),
        order_column=level_elem.get('ordinalColumn')
    )


def parse_measure(measure_elem) -> Measure:
    """Parse a Measure element."""
    agg_map = {
        'sum': 'SUM',
        'count': 'COUNT',
        'avg': 'AVG',
        'min': 'MIN',
        'max': 'MAX',
        'distinct-count': 'COUNT DISTINCT'
    }
    
    agg_type = measure_elem.get('aggregator', 'sum').lower()
    
    return Measure(
        name=measure_elem.get('name', 'UnnamedMeasure'),
        column=measure_elem.get('column', 'unknown_column'),
        agg=agg_map.get(agg_type, 'SUM')
    )

