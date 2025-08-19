import dlt
from pyspark.sql.types import ArrayType, FloatType, MapType, StringType, StructField, StructType, TimestampType


@dlt.table(
    name="nodes",
    comment="Graph nodes (vertices) table optimized for GraphFrames"
)
def create_nodes_table():
    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("entity_type", StringType(), nullable=True, 
                    metadata={"comment": "table|column|transformation|function"}),
        StructField("entity_name", StringType(), nullable=True),
        StructField("properties", MapType(StringType(), StringType()), nullable=True),
        StructField("embedding", ArrayType(FloatType()), nullable=True, 
                    metadata={"comment": "For Vector Search"}),
        StructField("created_at", TimestampType(), nullable=True)
    ])
    
    return spark.createDataFrame([], schema)
