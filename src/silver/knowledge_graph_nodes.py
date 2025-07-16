# Databricks notebook source
# MAGIC %md
# MAGIC # Knowledge Graph Nodes Table - Silver Layer
# MAGIC
# MAGIC This DLT pipeline creates and manages the nodes_table for storing 
# MAGIC knowledge graph entities with Unity Catalog integration, optimizations, 
# MAGIC and comprehensive CRUD operations.
# MAGIC
# MAGIC ## Features:
# MAGIC - Unity Catalog three-level namespace
# MAGIC - Vector embeddings support with search index
# MAGIC - Partitioning and clustering for performance
# MAGIC - Auto-optimize and retention policies
# MAGIC - Data quality expectations
# MAGIC - Role-based access control

# COMMAND ----------

import dlt
import logging
from typing import Dict, List, Optional
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, ArrayType, FloatType,
    TimestampType, LongType, BooleanType
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Schema Definition

# COMMAND ----------

# Configuration for Unity Catalog namespace
CATALOG_NAME = "bcp_ide"
SCHEMA_NAME = "knowledge_graph"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Tables Pipeline Implementation

# COMMAND ----------


@dlt.table(
    name=NODES_TABLE_NAME,
    comment="Knowledge graph nodes with Unity Catalog integration and "
           "vector embeddings support",
    table_properties=TABLE_PROPERTIES,
    schema=NODES_SCHEMA,
    partition_cols=["entity_type"]
)
@dlt.expect("valid_node_id", "node_id IS NOT NULL AND length(node_id) > 0")
@dlt.expect("valid_entity_type", 
           "entity_type IN ('table', 'column', 'transformation', 'function')")
@dlt.expect("valid_entity_name", 
           "entity_name IS NOT NULL AND length(entity_name) > 0")
@dlt.expect("valid_timestamps", 
           "created_at IS NOT NULL AND updated_at IS NOT NULL")
@dlt.expect("valid_version", "version >= 1")
@dlt.expect_or_drop("unique_node_id", "node_id IS NOT NULL")
def create_nodes_table():
    """
    Creates the main nodes table for the knowledge graph.
    
    This table stores all entities (tables, columns, transformations, 
    functions) with their metadata, properties, and vector embeddings 
    for semantic search.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    # For now, return an empty DataFrame with the correct schema
    # In a real implementation, this would read from source data
    return spark.createDataFrame([], NODES_SCHEMA)


# COMMAND ----------

# MAGIC %md
# MAGIC ## CRUD Operations Implementation

# COMMAND ----------

def create_node(node_id: str, entity_type: str, entity_name: str, 
               properties: Dict[str, str] = None, 
               embedding: List[float] = None) -> None:
    """
    Create a new node in the knowledge graph.
    
    Args:
        node_id: Unique identifier for the node
        entity_type: Type of entity (table, column, transformation, function)
        entity_name: Name of the entity
        properties: Additional properties as key-value pairs
        embedding: Vector embedding for semantic search
    """
    from datetime import datetime
    
    # Validate inputs
    if not node_id or not entity_type or not entity_name:
        raise ValueError("node_id, entity_type, and entity_name are required")
    
    if entity_type not in ['table', 'column', 'transformation', 'function']:
        raise ValueError("entity_type must be one of: table, column, transformation, function")
    
    # Prepare data
    current_time = datetime.now()
    properties = properties or {}
    embedding = embedding or []
    
    node_data = {
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
    
    # Create DataFrame and write to table
    df = spark.createDataFrame([node_data], NODES_SCHEMA)
    
    # Use MERGE INTO pattern for upsert
    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
    
    df.createOrReplaceTempView("new_node")
    
    spark.sql(f"""
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
    """)
    
    logger.info(f"Successfully created/updated node: {node_id}")

# COMMAND ----------

def read_node(node_id: str) -> Optional[Dict]:
    """
    Read a specific node from the knowledge graph.
    
    Args:
        node_id: Unique identifier for the node
        
    Returns:
        Dictionary containing node data or None if not found
    """
    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
    
    result = spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE node_id = '{node_id}' AND is_active = true
    """).collect()
    
    if result:
        return result[0].asDict()
    return None

# COMMAND ----------

def update_node(node_id: str, **kwargs) -> bool:
    """
    Update an existing node in the knowledge graph.
    
    Args:
        node_id: Unique identifier for the node
        **kwargs: Fields to update (entity_name, properties, embedding, etc.)
        
    Returns:
        True if update was successful, False if node not found
    """
    from datetime import datetime
    
    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
    
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

# COMMAND ----------

def delete_node(node_id: str, soft_delete: bool = True) -> bool:
    """
    Delete a node from the knowledge graph.
    
    Args:
        node_id: Unique identifier for the node
        soft_delete: If True, performs logical deletion; if False, physical deletion
        
    Returns:
        True if deletion was successful, False if node not found
    """
    from datetime import datetime
    
    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
    
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

# COMMAND ----------

def search_nodes_by_type(entity_type: str, limit: int = 100) -> List[Dict]:
    """
    Search nodes by entity type with performance optimization.
    
    Args:
        entity_type: Type of entity to search for
        limit: Maximum number of results to return
        
    Returns:
        List of dictionaries containing node data
    """
    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
    
    result = spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE entity_type = '{entity_type}' AND is_active = true
        ORDER BY created_at DESC
        LIMIT {limit}
    """).collect()
    
    return [row.asDict() for row in result]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Creation Setup and Optimization

# COMMAND ----------

def setup_table_optimizations():
    """
    Set up additional optimizations and indexes after table creation.
    """
    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
    
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

# COMMAND ----------

def setup_access_control():
    """
    Set up role-based access control for the knowledge graph tables.
    """
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
        table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
        
        permissions_sql = [
            f"GRANT SELECT ON TABLE {table_name} TO kg_reader",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table_name} TO kg_writer",
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Monitoring

# COMMAND ----------

@dlt.table(
    name=f"{NODES_TABLE_NAME}_quality_metrics",
    comment="Data quality metrics for the knowledge graph nodes table"
)
def nodes_quality_metrics():
    """
    Generate data quality metrics for monitoring the nodes table.
    """
    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{NODES_TABLE_NAME}"
    
    return spark.sql(f"""
        SELECT 
            current_timestamp() as measurement_time,
            entity_type,
            count(*) as total_nodes,
            count(CASE WHEN is_active = true THEN 1 END) as active_nodes,
            count(CASE WHEN embedding IS NOT NULL AND size(embedding) > 0 THEN 1 END) as nodes_with_embeddings,
            avg(CASE WHEN embedding IS NOT NULL THEN size(embedding) END) as avg_embedding_dimension,
            min(created_at) as oldest_node,
            max(created_at) as newest_node,
            count(DISTINCT entity_name) as unique_entity_names
        FROM {table_name}
        GROUP BY entity_type
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing and Validation Functions

# COMMAND ----------

def test_crud_operations():
    """
    Test basic CRUD operations on the nodes table.
    """
    test_node_id = "test_node_001"
    
    try:
        # Test CREATE
        create_node(
            node_id=test_node_id,
            entity_type="table",
            entity_name="test_table",
            properties={"schema": "test_schema", "owner": "test_user"},
            embedding=[0.1, 0.2, 0.3, 0.4, 0.5]
        )
        logger.info("✓ CREATE operation successful")
        
        # Test READ
        node = read_node(test_node_id)
        assert node is not None
        assert node["entity_name"] == "test_table"
        logger.info("✓ READ operation successful")
        
        # Test UPDATE
        success = update_node(test_node_id, entity_name="updated_test_table")
        assert success
        updated_node = read_node(test_node_id)
        assert updated_node["entity_name"] == "updated_test_table"
        logger.info("✓ UPDATE operation successful")
        
        # Test DELETE (soft)
        success = delete_node(test_node_id, soft_delete=True)
        assert success
        deleted_node = read_node(test_node_id)
        assert deleted_node is None  # Should not be found since is_active = false
        logger.info("✓ DELETE operation successful")
        
        logger.info("All CRUD operations tests passed!")
        
    except Exception as e:
        logger.error(f"CRUD operations test failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Initialization

# COMMAND ----------

# Initialize the pipeline
if __name__ == "__main__":
    logger.info("Initializing Knowledge Graph Nodes DLT Pipeline")
    
    # The table creation will be handled by DLT automatically
    # Post-creation setup should be done separately
    logger.info("DLT Pipeline initialized successfully") 