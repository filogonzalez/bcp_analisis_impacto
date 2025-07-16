"""
Knowledge Graph Nodes Table Utilities

This module provides utility functions and schema definitions for the 
knowledge graph nodes table DLT pipeline.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, ArrayType, FloatType,
    TimestampType, LongType, BooleanType
)

# Configure logging
logger = logging.getLogger(__name__)

# Configuration constants
CATALOG_NAME = "bcp_ide"
SCHEMA_NAME = "codebase_knowledge_graph" 
NODES_TABLE_NAME = "nodes_table"

# Table properties for optimization
TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.deletedFileRetentionDuration": "365 days",
    "delta.enableChangeDataFeed": "true",
    "delta.feature.allowColumnDefaults": "enabled"
}

# Schema definition for nodes_table
NODES_SCHEMA = StructType([
    StructField("node_id", StringType(), False),
    StructField("entity_type", StringType(), False),
    StructField("entity_name", StringType(), False),
    StructField("properties", MapType(StringType(), StringType()), True),
    StructField("embedding", ArrayType(FloatType()), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("version", LongType(), False),
    StructField("is_active", BooleanType(), False)
])

# Valid entity types
VALID_ENTITY_TYPES = ['table', 'column', 'transformation', 'function']


def get_table_name() -> str:
    """Get the full table name with Unity Catalog namespace."""
    return f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"


def validate_entity_type(entity_type: str) -> None:
    """Validate that entity_type is one of the allowed values."""
    if entity_type not in VALID_ENTITY_TYPES:
        raise ValueError(
            f"entity_type must be one of: {', '.join(VALID_ENTITY_TYPES)}"
        )


def create_node(node_id: str, entity_type: str, entity_name: str,
                properties: Optional[Dict[str, str]] = None,
                embedding: Optional[List[float]] = None) -> Dict:
    """
    Create a new node record.
    
    Args:
        node_id: Unique identifier for the node
        entity_type: Type of entity (table, column, transformation, function)
        entity_name: Name of the entity
        properties: Additional properties as key-value pairs
        embedding: Vector embedding for semantic search
        
    Returns:
        Dictionary representing the node record
        
    Raises:
        ValueError: If required fields are missing or invalid
    """
    # Validate inputs
    if not node_id or not entity_type or not entity_name:
        raise ValueError("node_id, entity_type, and entity_name are required")
    
    validate_entity_type(entity_type)
    
    # Prepare data
    current_time = datetime.now()
    properties = properties or {}
    embedding = embedding or []
    
    return {
        "node_id": node_id,
        "entity_type": entity_type,
        "entity_name": entity_name,
        "properties": properties,
        "embedding": embedding,
        "created_at": current_time,
        "updated_at": current_time,
        "version": 1,
        "is_active": True
    }


def create_node_sql(node_id: str) -> None:
    """
    Create/update a node using SQL MERGE pattern.
    
    Args:
        node_id: Unique identifier for the node to create/update
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    merge_sql = f"""
        MERGE INTO {table_name} target
        USING new_node source
        ON target.node_id = source.node_id
        WHEN MATCHED THEN
            UPDATE SET
                entity_name = source.entity_name,
                properties = source.properties,
                embedding = source.embedding,
                updated_at = source.updated_at,
                version = target.version + 1,
                is_active = source.is_active
        WHEN NOT MATCHED THEN
            INSERT (node_id, entity_type, entity_name, properties, embedding, 
                   created_at, updated_at, version, is_active)
            VALUES (source.node_id, source.entity_type, source.entity_name, 
                   source.properties, source.embedding, source.created_at, 
                   source.updated_at, source.version, source.is_active)
    """
    
    spark.sql(merge_sql)
    logger.info(f"Successfully created/updated node: {node_id}")


def read_node(node_id: str) -> Optional[Dict]:
    """
    Read a specific node from the knowledge graph.
    
    Args:
        node_id: Unique identifier for the node
        
    Returns:
        Dictionary containing node data or None if not found
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    result = spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE node_id = '{node_id}' AND is_active = true
    """).collect()
    
    if result:
        return result[0].asDict()
    return None


def update_node_sql(node_id: str, **kwargs) -> bool:
    """
    Update an existing node in the knowledge graph.
    
    Args:
        node_id: Unique identifier for the node
        **kwargs: Fields to update (entity_name, properties, embedding, etc.)
        
    Returns:
        True if update was successful, False if node not found
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    # Check if node exists
    if not read_node(node_id):
        logger.warning(f"Node {node_id} not found for update")
        return False
    
    # Build update statement
    update_fields = []
    for field, value in kwargs.items():
        if field in ['entity_name', 'properties', 'embedding']:
            if isinstance(value, str):
                update_fields.append(f"{field} = '{value}'")
            else:
                update_fields.append(f"{field} = {value}")
    
    update_fields.append(f"updated_at = '{datetime.now()}'")
    update_fields.append("version = version + 1")
    
    update_clause = ", ".join(update_fields)
    
    spark.sql(f"""
        UPDATE {table_name}
        SET {update_clause}
        WHERE node_id = '{node_id}' AND is_active = true
    """)
    
    logger.info(f"Successfully updated node: {node_id}")
    return True


def delete_node_sql(node_id: str, soft_delete: bool = True) -> bool:
    """
    Delete a node from the knowledge graph.
    
    Args:
        node_id: Unique identifier for the node
        soft_delete: If True, performs logical deletion; 
                    if False, physical deletion
        
    Returns:
        True if deletion was successful, False if node not found
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    # Check if node exists
    if not read_node(node_id):
        logger.warning(f"Node {node_id} not found for deletion")
        return False
    
    if soft_delete:
        # Logical deletion
        spark.sql(f"""
            UPDATE {table_name}
            SET is_active = false, updated_at = '{datetime.now()}'
            WHERE node_id = '{node_id}'
        """)
        logger.info(f"Successfully soft-deleted node: {node_id}")
    else:
        # Physical deletion
        spark.sql(f"""
            DELETE FROM {table_name}
            WHERE node_id = '{node_id}'
        """)
        logger.info(f"Successfully hard-deleted node: {node_id}")
    
    return True


def search_nodes_by_type(entity_type: str, limit: int = 100) -> List[Dict]:
    """
    Search nodes by entity type with performance optimization.
    
    Args:
        entity_type: Type of entity to search for
        limit: Maximum number of results to return
        
    Returns:
        List of dictionaries containing node data
    """
    validate_entity_type(entity_type)
    
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    result = spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE entity_type = '{entity_type}' AND is_active = true
        ORDER BY created_at DESC
        LIMIT {limit}
    """).collect()
    
    return [row.asDict() for row in result]


def setup_table_optimizations() -> None:
    """Set up additional optimizations and indexes after table creation."""
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    try:
        # Apply clustering for better query performance
        spark.sql(f"""
            ALTER TABLE {table_name} 
            CLUSTER BY (entity_name)
        """)
        
        # Create vector search index for embeddings (if supported)
        try:
            spark.sql(f"""
                CREATE INDEX IF NOT EXISTS node_vector_idx
                ON TABLE {table_name}
                USING VECTOR
                ON embedding
                OPTIONS (
                    'vector_column_name' = 'embedding',
                    'dimensions' = '768',
                    'metric_type' = 'cosine',
                    'index_type' = 'IVF_PQ'
                )
            """)
            logger.info("Vector search index created successfully")
        except Exception as e:
            logger.warning(f"Vector search index creation failed: {e}")
        
        logger.info("Table optimizations applied successfully")
        
    except Exception as e:
        logger.error(f"Error applying table optimizations: {e}")


def setup_access_control() -> None:
    """Set up role-based access control for the knowledge graph tables."""
    spark = SparkSession.getActiveSession()
    
    try:
        # Create roles for knowledge graph access
        roles_sql = [
            "CREATE ROLE IF NOT EXISTS kg_reader",
            "CREATE ROLE IF NOT EXISTS kg_writer", 
            "CREATE ROLE IF NOT EXISTS kg_admin"
        ]
        
        for sql in roles_sql:
            try:
                spark.sql(sql)
            except Exception as e:
                logger.warning(f"Role creation failed: {e}")
        
        # Grant permissions
        table_name = get_table_name()
        
        permissions_sql = [
            f"GRANT SELECT ON TABLE {table_name} TO kg_reader",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table_name} "
            f"TO kg_writer",
            f"GRANT ALL PRIVILEGES ON TABLE {table_name} TO kg_admin"
        ]
        
        for sql in permissions_sql:
            try:
                spark.sql(sql)
            except Exception as e:
                logger.warning(f"Permission grant failed: {e}")
        
        logger.info("Access control setup completed")
        
    except Exception as e:
        logger.error(f"Error setting up access control: {e}")


def test_crud_operations() -> None:
    """Test basic CRUD operations on the nodes table."""
    test_node_id = "test_node_001"
    
    try:
        # Test CREATE
        node_data = create_node(
            node_id=test_node_id,
            entity_type="table",
            entity_name="test_table",
            properties={"schema": "test_schema", "owner": "test_user"},
            embedding=[0.1, 0.2, 0.3, 0.4, 0.5]
        )
        
        # Create DataFrame and test SQL operations
        spark = SparkSession.getActiveSession()
        df = spark.createDataFrame([node_data], NODES_SCHEMA)
        df.createOrReplaceTempView("new_node")
        
        create_node_sql(test_node_id)
        logger.info("✓ CREATE operation successful")
        
        # Test READ
        node = read_node(test_node_id)
        assert node is not None
        assert node["entity_name"] == "test_table"
        logger.info("✓ READ operation successful")
        
        # Test UPDATE
        success = update_node_sql(test_node_id, entity_name="updated_test_table")
        assert success
        updated_node = read_node(test_node_id)
        assert updated_node["entity_name"] == "updated_test_table"
        logger.info("✓ UPDATE operation successful")
        
        # Test DELETE (soft)
        success = delete_node_sql(test_node_id, soft_delete=True)
        assert success
        deleted_node = read_node(test_node_id)
        assert deleted_node is None  # Should not be found since is_active = false
        logger.info("✓ DELETE operation successful")
        
        logger.info("All CRUD operations tests passed!")
        
    except Exception as e:
        logger.error(f"CRUD operations test failed: {e}")
        raise 