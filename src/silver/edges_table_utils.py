"""
Knowledge Graph Edges Table Utilities

This module provides utility functions and schema definitions for the 
edges_table DLT pipeline.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, ArrayType, FloatType,
    TimestampType, LongType, BooleanType, DoubleType
)

# Configure logging
logger = logging.getLogger(__name__)

# Configuration constants
CATALOG_NAME = "bcp_ide"
SCHEMA_NAME = "codebase_knowledge_graph"
EDGES_TABLE_NAME = "edges_table"
NODES_TABLE_NAME = "nodes_table"

# Table properties for optimization
TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.deletedFileRetentionDuration": "365 days",
    "delta.enableChangeDataFeed": "true",
    "delta.feature.allowColumnDefaults": "enabled"
}

# Schema definition for edges_table
EDGES_SCHEMA = StructType([
    StructField("edge_id", StringType(), False),
    StructField("source_node_id", StringType(), False),
    StructField("target_node_id", StringType(), False),
    StructField("relationship_type", StringType(), False),
    StructField("properties", MapType(StringType(), StringType()), True),
    StructField("confidence_score", DoubleType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("valid_from", TimestampType(), True),
    StructField("valid_to", TimestampType(), True),
    StructField("version", LongType(), False),
    StructField("is_active", BooleanType(), False)
])

# Valid relationship types (example, can be extended)
VALID_RELATIONSHIP_TYPES = [
    "depends_on", "transforms", "reads", "writes", "calls", "contains"
]


def get_table_name() -> str:
    """Get the full table name with Unity Catalog namespace."""
    return f"{CATALOG_NAME}.{SCHEMA_NAME}.{EDGES_TABLE_NAME}"


def create_edge(edge_id: str, source_node_id: str, target_node_id: str,
                relationship_type: str, properties: Optional[Dict[str, str]] = None,
                confidence_score: Optional[float] = None,
                valid_from: Optional[datetime] = None,
                valid_to: Optional[datetime] = None) -> Dict:
    """
    Create a new edge record.
    """
    if not edge_id or not source_node_id or not target_node_id or not relationship_type:
        raise ValueError("edge_id, source_node_id, target_node_id, and relationship_type are required")
    
    current_time = datetime.now()
    properties = properties or {}
    
    return {
        "edge_id": edge_id,
        "source_node_id": source_node_id,
        "target_node_id": target_node_id,
        "relationship_type": relationship_type,
        "properties": properties,
        "confidence_score": confidence_score,
        "created_at": current_time,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "version": 1,
        "is_active": True
    }


def create_edge_sql(edge_id: str) -> None:
    """
    Create/update an edge using SQL MERGE pattern.
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    merge_sql = f"""
        MERGE INTO {table_name} target
        USING new_edge source
        ON target.edge_id = source.edge_id
        WHEN MATCHED THEN
            UPDATE SET
                source_node_id = source.source_node_id,
                target_node_id = source.target_node_id,
                relationship_type = source.relationship_type,
                properties = source.properties,
                confidence_score = source.confidence_score,
                valid_from = source.valid_from,
                valid_to = source.valid_to,
                updated_at = source.updated_at,
                version = target.version + 1,
                is_active = source.is_active
        WHEN NOT MATCHED THEN
            INSERT (edge_id, source_node_id, target_node_id, relationship_type, properties, confidence_score, created_at, valid_from, valid_to, version, is_active)
            VALUES (source.edge_id, source.source_node_id, source.target_node_id, source.relationship_type, source.properties, source.confidence_score, source.created_at, source.valid_from, source.valid_to, source.version, source.is_active)
    """
    spark.sql(merge_sql)
    logger.info(f"Successfully created/updated edge: {edge_id}")


def read_edge(edge_id: str) -> Optional[Dict]:
    """
    Read a specific edge from the knowledge graph.
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    result = spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE edge_id = '{edge_id}' AND is_active = true
    """).collect()
    if result:
        return result[0].asDict()
    return None


def update_edge_sql(edge_id: str, **kwargs) -> bool:
    """
    Update an existing edge in the knowledge graph.
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    if not read_edge(edge_id):
        logger.warning(f"Edge {edge_id} not found for update")
        return False
    update_fields = []
    for field, value in kwargs.items():
        if field in ['source_node_id', 'target_node_id', 'relationship_type', 'properties', 'confidence_score', 'valid_from', 'valid_to']:
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
        WHERE edge_id = '{edge_id}' AND is_active = true
    """)
    logger.info(f"Successfully updated edge: {edge_id}")
    return True


def delete_edge_sql(edge_id: str, soft_delete: bool = True) -> bool:
    """
    Delete an edge from the knowledge graph.
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    if not read_edge(edge_id):
        logger.warning(f"Edge {edge_id} not found for deletion")
        return False
    if soft_delete:
        spark.sql(f"""
            UPDATE {table_name}
            SET is_active = false, updated_at = '{datetime.now()}'
            WHERE edge_id = '{edge_id}'
        """)
        logger.info(f"Successfully soft-deleted edge: {edge_id}")
    else:
        spark.sql(f"""
            DELETE FROM {table_name}
            WHERE edge_id = '{edge_id}'
        """)
        logger.info(f"Successfully hard-deleted edge: {edge_id}")
    return True


def search_edges_by_type(relationship_type: str, limit: int = 100) -> List[Dict]:
    """
    Search edges by relationship type.
    """
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    result = spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE relationship_type = '{relationship_type}' AND is_active = true
        ORDER BY created_at DESC
        LIMIT {limit}
    """).collect()
    return [row.asDict() for row in result]


def setup_table_optimizations() -> None:
    """Set up clustering and vector index after table creation."""
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    try:
        # Apply clustering for better query performance
        spark.sql(f"""
            ALTER TABLE {table_name} 
            CLUSTER BY (source_node_id, relationship_type)
        """)
        logger.info("Clustering applied successfully")
        # Create specialized indexes for graph traversal if needed
        # (Placeholder for future index creation)
    except Exception as e:
        logger.error(f"Error applying table optimizations: {e}")


def setup_access_control() -> None:
    """Set up role-based access control for the edges table."""
    spark = SparkSession.getActiveSession()
    try:
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
        table_name = get_table_name()
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


def test_crud_operations() -> None:
    """Test basic CRUD operations on the edges table."""
    test_edge_id = "test_edge_001"
    try:
        edge_data = create_edge(
            edge_id=test_edge_id,
            source_node_id="test_node_001",
            target_node_id="test_node_002",
            relationship_type="depends_on",
            properties={"weight": "0.8"},
            confidence_score=0.8
        )
        spark = SparkSession.getActiveSession()
        df = spark.createDataFrame([edge_data], EDGES_SCHEMA)
        df.createOrReplaceTempView("new_edge")
        create_edge_sql(test_edge_id)
        logger.info("✓ CREATE operation successful")
        edge = read_edge(test_edge_id)
        assert edge is not None
        assert edge["relationship_type"] == "depends_on"
        logger.info("✓ READ operation successful")
        success = update_edge_sql(test_edge_id, relationship_type="transforms")
        assert success
        updated_edge = read_edge(test_edge_id)
        assert updated_edge["relationship_type"] == "transforms"
        logger.info("✓ UPDATE operation successful")
        success = delete_edge_sql(test_edge_id, soft_delete=True)
        assert success
        deleted_edge = read_edge(test_edge_id)
        assert deleted_edge is None
        logger.info("✓ DELETE operation successful")
        logger.info("All CRUD operations tests passed!")
    except Exception as e:
        logger.error(f"CRUD operations test failed: {e}")
        raise 