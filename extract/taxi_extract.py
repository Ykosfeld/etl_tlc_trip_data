from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import logging
from config.schemas import yellow_taxi_schema, green_taxi_schema

logger = logging.getLogger(__name__)


def extract_taxi_data(
    spark: SparkSession,
    path: str,
    schema: StructType
) -> DataFrame:
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

