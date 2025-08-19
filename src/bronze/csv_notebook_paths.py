# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr, current_timestamp

@dlt.table
def bronze_paths():
    df = (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.allowOverwrites", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("mergeSchema", "true")
            .option("header", "true")
            .load("/Volumes/bcp_ide/codebase_knowledge_graph/nb_paths")
    )
    df = df.withColumn("processing_time", current_timestamp())
    return df

# COMMAND ----------

# Create a streaming table for soft invalid XML details data with SCD Type 1 updates and deletes /
# Crear una tabla de streaming para datos XML invalidados suaves con SCD Type 1 actualizaciones y eliminaciones
dlt.create_streaming_table(
    name="notebook_paths",
    comment="SCD Type 1 notebook paths extracted from csv",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true"
    },
    # expect_all_or_drop=validation_expectations
)

dlt.apply_changes(
    target="notebook_paths",
    source="bronze_paths",
    keys=["entity_id"],
    sequence_by=col("fechora_ultima_ejecucion_job"),
    stored_as_scd_type=1
)