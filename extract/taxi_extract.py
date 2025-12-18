from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import logging
from config.schemas import yellow_taxi_schema, green_taxi_schema

logger = logging.getLogger(__name__)


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

    yellow_taxi_trips = extract_taxi_data(spark, "data/raw/yellow_tripdata_2025-10.parquet", yellow_taxi_schema)
    green_taxi_trips = extract_taxi_data(spark, "data/raw/green_tripdata_2025-10.parquet", green_taxi_schema)
    
    all_taxi_trips = yellow_taxi_trips.unionByName(green_taxi_trips, allowMissingColumns=True)

    total_trips = all_taxi_trips.count()
    logger.info(f"O DataFrame combinado contém {total_trips} corridas")

    return all_taxi_trips
