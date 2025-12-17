from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Taxi ETL") \
        .config("spark.sql.adpative.enabled", "true") \
        .getOrCreate()
