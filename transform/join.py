from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

logger = logging.getLogger(__name__)

def join_dataframes(
    df1: DataFrame,
    df2: DataFrame
) -> DataFrame:
    """Une dois DataFrames Spark podendo ter colunas faltantes e remove as colunas 'ehail_fee' e 'trip_type'

    Args:
        df1 (DataFrame): DataFrame Spark
        df2 (DataFrame): DataFrame Spark

    Returns:
        DataFrame: DataFrame Spark resultante da união e remoção das colunas
    """
    return df1.unionByName(df2, allowMissingColumns=True).drop("ehail_fee", "trip_type")

