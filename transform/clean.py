from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

import config.schemas as schemas

logger = logging.getLogger(__name__)

def coalesce_timestamp(df: DataFrame, output_col: str, yellow_cab_col: str, green_cab_col: str) -> DataFrame:
    """Função que agrega as entradas de timestamp que estão em duas colunas diferentes
     em uma coluna nova e remove as duas colunas redundates originais

    Args:
        df (DataFrame): DataFrame Spark alvo
        output_col (str): Nome da nova coluna agregadora
        yellow_cab_col (str): Nome da coluna que possue as entradas de timestamp vindo dos dados de taxis amarelos
        green_cab_col (str): Nome da coluna que possue as entradas de timestamp vindo dos dados de taxis verdes

    Returns:
        DataFrame: Novo DataFrame com a nova coluna e sem as duas colunas redundantes
    """
    return df.withColumn(
        output_col, 
        coalesce(col(yellow_cab_col), col(green_cab_col))
    ).drop(yellow_cab_col, green_cab_col)

def replace_value_with_null(df: DataFrame, column: str, invalid_value: int) -> DataFrame:
    """Função que substitui as entradas de um valor indesejado para Null em uma coluna desejada

    Args:
        df (DataFrame): DataFrame Spark alvo
        column (str): Coluna alvo
        invalid_value (int): Valor indesejado

    Returns:
        DataFrame: Novo DataFrame Spark com a substituição realizada
    """
    
    return df.withColumn(
        column,
        when(col(column) == invalid_value, None)
        .otherwise(col(column))
    ) 

def replace_null_with_value(df: DataFrame, column: str, correct_value: int | float) -> DataFrame:
    """Função que substitui as entradas Null para um valo desejado em uma coluna desejada

    Args:
        df (DataFrame): DataFrame Spark alvo
        column (str): Coluna alvo
        correct_value (int | float): Valor desejado

    Returns:
        DataFrame: Novo DataFrame Spark com a substituição realizada
    """
    
    return df.withColumn(
        column,
        when(col(column).isNull(), correct_value)
        .otherwise(col(column))
    )

def rename_timestamp_column(
    df: DataFrame, 
    possible_names: list[str], 
    final_name: str
) -> DataFrame:
    for col in possible_names:
        if col in df.columns:
            return df.withColumnRenamed(col, final_name)
    return df

def final_clean(df: DataFrame) -> DataFrame:
    """Limpa o DataFrame Spark desejado realizando as seguintes ações na ordem:
       1- Agregas as colunas de pickup_time e dropoff_time dos taxis amarelos e verdes em duas colunas: pickup_time e dropoff_time
       2- Substitui as entradas desconhecidas das colunas 'payment_type' e 'RatecodeID' para Null
       3- Substitui as entradas Null das colunas 'Airport_fee' e 'congestion_surcharge' para 0.0
       4- Remove as colunas 'ehail_fee' e 'trip_type'

    Args:
        df (DataFrame): DataFrame Spark alvo

    Returns:
        DataFrame: Novo DataFrame Spark limpo
    """
    
    clean_df = rename_timestamp_column(df, ["tpep_pickup_datetime", "lpep_pickup_datetime"], "pickup_datetime")
    clean_df = rename_timestamp_column(clean_df, ["tpep_dropoff_datetime", "lpep_dropoff_datetime"], "dropoff_datetime")

    clean_df = replace_value_with_null(clean_df, "payment_type", 5)
    clean_df = replace_value_with_null(clean_df, "RatecodeID", 99)

    if "Airport_fee" in clean_df.columns:
        clean_df = replace_null_with_value(clean_df, "Airport_fee", 0.0)
    if "congestion_surcharge" in clean_df.columns:
        clean_df = replace_null_with_value(clean_df, "congestion_surcharge", 0.0)

    return clean_df