from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

yellow_taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])

green_taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])

def extract_taxi_data(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Cria um DataFrame Spark com schema explícito para dados de corridas de taxi.

    Args:
        spark (SparkSession): sessão do spark
        path (str): caminho para os dados
        schema (StructType): schema para o DataFrame que será carregado

    Returns:
        DataFrame: DataFrame já estruturado de acordo com o schema dado
    """

    logger.info(f"Lendo dados de taxis de {path}")
    
    df = spark.read \
        .schema(schema) \
        .option("header", "true") \
        .parquet(path)
    
    total_trips = df.count()
    logger.info(f"Total de corridas carregadas: {total_trips}")

    return df

def extract_all_taxi_data(spark: SparkSession) -> DataFrame:
    """Cria um DataFrame Spark das corridas combinads de taxis amarelos e verdes.

    Args:
        spark (SparkSession): sessão do spark

    Returns:
        DataFrame: DataFrame resultante da união
    """
    
    yellow_taxi_trips = extract_taxi_data(spark, "../data/yellow_tripdata_2025-10.parquet", yellow_taxi_schema)
    green_taxi_trips = extract_taxi_data(spark, "../data/green_tripdata_2025-10.parquet", green_taxi_schema)
    
    all_taxi_trips = yellow_taxi_trips.unionByName(green_taxi_trips)

    total_trips = all_taxi_trips.count()
    logger.info(f"O DataFrame combinado contém {total_trips} corridas")

    return all_taxi_trips
