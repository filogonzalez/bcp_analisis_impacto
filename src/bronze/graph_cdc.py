import dlt
from pyspark.sql.functions import col


dlt.create_streaming_table(
    name="graph_nodes",
    comment="SCD Type 1 graph nodes from AUTO CDC",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="graph_nodes",
    source="nodes",
    keys=["id"],
    sequence_by=col("created_at"),
    apply_as_deletes=None,
    except_column_list=[],
    stored_as_scd_type=1
)


dlt.create_streaming_table(
    name="graph_edges",
    comment="SCD Type 1 graph edges from AUTO CDC",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.targetFileSize": "67108864",
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="graph_edges",
    source="edges",
    keys=["src", "dst", "relationship"],
    sequence_by=col("created_at"),
    apply_as_deletes=None,
    except_column_list=[],
    stored_as_scd_type=1
)
