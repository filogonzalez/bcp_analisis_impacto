import dlt
from pyspark.sql.types import FloatType, MapType, StringType, StructField, StructType, TimestampType  # noqa: E501


@dlt.table(
    name="edges",
    comment="Graph edges table optimized for GraphFrames",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true" # Z-order by src,dst for traversal performance  # noqa: E501
    }
)
@dlt.expect_all({
    "valid_src": "src IS NOT NULL",
    "valid_dst": "dst IS NOT NULL",
    "valid_relationship": "relationship IN ('reads_from', 'writes_to', 'transforms', 'contains')"  # noqa: E501
})
def create_edges_table():
    schema = StructType([
        StructField("src", StringType(), nullable=False, 
                    metadata={"comment": "Source node ID (required for GraphFrames)"}),  # noqa: E501
        StructField("dst", StringType(), nullable=False, 
                    metadata={"comment": "Destination node ID (required for GraphFrames)"}),  # noqa: E501
        StructField("relationship", StringType(), nullable=True, 
                    metadata={"comment": "reads_from|writes_to|transforms|contains"}),  # noqa: E501
        StructField("weight", FloatType(), nullable=True,
                    metadata={"comment": "Edge weight for algorithms like PageRank"}),  # noqa: E501
        StructField("properties", MapType(StringType(), StringType()), nullable=True,
                    metadata={"comment": "Additional edge properties"}),  # noqa: E501
        StructField("created_at", TimestampType(), nullable=True,
                    metadata={"comment": "Timestamp when the edge was created"}),  # noqa: E501
        StructField("updated_at", TimestampType(), nullable=True,
                    metadata={"comment": "Timestamp when the edge was last updated"})  # noqa: E501
    ])
    
    return spark.createDataFrame([], schema)  # noqa: F821
