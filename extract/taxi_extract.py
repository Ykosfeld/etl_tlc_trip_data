from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

def read_taxi_data(
    spark: SparkSession,
    path: str,
    schema: StructType
) -> DataFrame:
    return (
        spark.read
        .schema(schema)
        .option("header", "true")
        .parquet(path)
    )

