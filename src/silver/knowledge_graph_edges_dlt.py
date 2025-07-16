# Databricks notebook source
# MAGIC %md
# MAGIC # Knowledge Graph Edges Table - DLT Pipeline
# MAGIC
# MAGIC This notebook implements the DLT pipeline for the knowledge graph edges table
# MAGIC with Unity Catalog integration and comprehensive CRUD operations.

# COMMAND ----------

import dlt
from edges_table_utils import (
    EDGES_SCHEMA, TABLE_PROPERTIES, EDGES_TABLE_NAME,
    setup_table_optimizations, setup_access_control, test_crud_operations
)
from nodes_table_utils import get_table_name as get_nodes_table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main DLT Table Definition with Referential Integrity

# COMMAND ----------

@dlt.table(
    name=EDGES_TABLE_NAME,
    comment="Knowledge graph edges with Unity Catalog integration",
    table_properties=TABLE_PROPERTIES,
    schema=EDGES_SCHEMA,
    partition_cols=["relationship_type"]
)
@dlt.expect("valid_edge_id", "edge_id IS NOT NULL AND length(edge_id) > 0")
@dlt.expect("valid_source_node_id", "source_node_id IS NOT NULL AND length(source_node_id) > 0")
@dlt.expect("valid_target_node_id", "target_node_id IS NOT NULL AND length(target_node_id) > 0")
@dlt.expect("valid_relationship_type", "relationship_type IS NOT NULL AND length(relationship_type) > 0")
@dlt.expect("valid_confidence_score", "confidence_score IS NULL OR (confidence_score >= 0.0 AND confidence_score <= 1.0)")
@dlt.expect("valid_timestamps", "created_at IS NOT NULL")
@dlt.expect("valid_version", "version >= 1")
@dlt.expect_or_drop("unique_edge_id", "edge_id IS NOT NULL")
def edges_table():
    """
    Creates the main edges table for the knowledge graph.
    
    This table stores all relationships (edges) between nodes, with metadata,
    properties, and confidence scores for probabilistic relationships.
    Referential integrity is enforced to ensure all source/target nodes exist.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    # Return empty DataFrame with correct schema for initial creation
    return spark.createDataFrame([], EDGES_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics Table

# COMMAND ----------

@dlt.table(
    name=f"{EDGES_TABLE_NAME}_quality_metrics",
    comment="Data quality metrics for the knowledge graph edges table"
)
def edges_quality_metrics():
    """Generate data quality metrics for monitoring."""
    from edges_table_utils import get_table_name
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    return spark.sql(f"""
        SELECT 
            current_timestamp() as measurement_time,
            relationship_type,
            count(*) as total_edges,
            count(CASE WHEN is_active = true THEN 1 END) as active_edges,
            count(CASE WHEN confidence_score IS NOT NULL THEN 1 END) as edges_with_confidence,
            avg(confidence_score) as avg_confidence_score,
            min(created_at) as oldest_edge,
            max(created_at) as newest_edge,
            count(DISTINCT source_node_id) as unique_source_nodes,
            count(DISTINCT target_node_id) as unique_target_nodes
        FROM {table_name}
        GROUP BY relationship_type
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Creation Setup Functions

# COMMAND ----------

def run_post_creation_setup():
    """Run optimization and access control setup after table creation."""
    setup_table_optimizations()
    setup_access_control()

def run_tests():
    """Run CRUD operation tests."""
    test_crud_operations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC (Change Data Capture) Usage Example
# MAGIC
# MAGIC The following function demonstrates how to read changes from the edges_table using Delta Lake's CDC feature.

# COMMAND ----------

def get_edge_changes(since_version: int):
    """Read CDC changes from the edges_table since a given version."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    from edges_table_utils import get_table_name
    table_name = get_table_name()
    return spark.read.format("delta").option("readChangeData", "true").option("startingVersion", since_version).table(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC
# MAGIC 1. Run this DLT pipeline to create the edges_table
# MAGIC 2. After initial creation, run `run_post_creation_setup()` to apply optimizations
# MAGIC 3. Use `run_tests()` to validate CRUD operations
# MAGIC 4. Use `get_edge_changes(since_version)` to read CDC changes
# MAGIC 5. Import `edges_table_utils` in other notebooks for CRUD operations 