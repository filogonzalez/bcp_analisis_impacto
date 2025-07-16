# Databricks notebook source
# MAGIC %md
# MAGIC # Knowledge Graph Nodes Table - DLT Pipeline
# MAGIC
# MAGIC This notebook implements the DLT pipeline for the knowledge graph nodes table
# MAGIC with Unity Catalog integration and comprehensive CRUD operations.

# COMMAND ----------

import dlt
from nodes_table_utils import (
    NODES_SCHEMA, TABLE_PROPERTIES, NODES_TABLE_NAME,
    setup_table_optimizations, setup_access_control, test_crud_operations
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main DLT Table Definition

# COMMAND ----------

@dlt.table(
    name=NODES_TABLE_NAME,
    comment="Knowledge graph nodes with Unity Catalog integration",
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
def nodes_table():
    """
    Creates the main nodes table for the knowledge graph.
    
    This table stores all entities with their metadata, properties,
    and vector embeddings for semantic search.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    # Return empty DataFrame with correct schema for initial creation
    return spark.createDataFrame([], NODES_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics Table

# COMMAND ----------

@dlt.table(
    name=f"{NODES_TABLE_NAME}_quality_metrics",
    comment="Data quality metrics for the knowledge graph nodes table"
)
def nodes_quality_metrics():
    """Generate data quality metrics for monitoring."""
    from nodes_table_utils import get_table_name
    from pyspark.sql import SparkSession
    
    spark = SparkSession.getActiveSession()
    table_name = get_table_name()
    
    return spark.sql(f"""
        SELECT 
            current_timestamp() as measurement_time,
            entity_type,
            count(*) as total_nodes,
            count(CASE WHEN is_active = true THEN 1 END) as active_nodes,
            count(CASE WHEN embedding IS NOT NULL AND size(embedding) > 0 
                  THEN 1 END) as nodes_with_embeddings,
            avg(CASE WHEN embedding IS NOT NULL 
                THEN size(embedding) END) as avg_embedding_dimension,
            min(created_at) as oldest_node,
            max(created_at) as newest_node,
            count(DISTINCT entity_name) as unique_entity_names
        FROM {table_name}
        GROUP BY entity_type
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
# MAGIC ## Usage Instructions
# MAGIC
# MAGIC 1. Run this DLT pipeline to create the nodes_table
# MAGIC 2. After initial creation, run `run_post_creation_setup()` to apply optimizations
# MAGIC 3. Use `run_tests()` to validate CRUD operations
# MAGIC 4. Import `nodes_table_utils` in other notebooks for CRUD operations 