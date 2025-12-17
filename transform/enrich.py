from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

logger = logging.getLogger(__name__)

""" TODO
1- adicionar coluna de data da corrida
2- adicionar coluna com duração da corrida X
3- adicionar coluna com total de impostos pago X
4- coluna de turno X
5- coluna dia da semana X

Agragar mudanças 

"""

def add_total_trip_duration(df: DataFrame, pickup_col: str, dropoff_col: str) -> DataFrame:
    """Adciona a duração total em segunda de cada corrida
     em uma coluna nova ao DataFrame Spark alvo 

    Args:
        df (DataFrame): DataFrame Spark alvo
        pickup_col (str): Nome da coluna que contem o timestamp do inicio da corrida
        dropoff_col (str): Nome da coluna que contem o timestamp do final da corrida

    Returns:
        DataFrame: Novo DataFrame Spark com a coluna da duração da corrida
    """
    return df.withColumn(
        "trip_duration",
        expr(f"timestampdiff(SECOND, {pickup_col}, {dropoff_col})")
    )

def add_total_taxes(df: DataFrame, fare_amount_col: str, total_amount_col: str) -> DataFrame:
    """Adciona o total pago em taxas/impostos na corrida 
    em uma coluna nova ao DataFrame Spark alvo

    Args:
        df (DataFrame): DataFrame Spark alvo
        fare_amount_col (str): Nome da coluna que contem o valor da corrida
        total_amount_col (str): Nome da coluna que contem o valor total pago

    Returns:
        DataFrame: Novo DataFrame Spark com a coluna taxas/impostos pagos
    """         
    return df.withColumn(
        "total_taxes",
        col(total_amount_col) - col(fare_amount_col)
    )

def add_hour_bucket(df: DataFrame, timestamp_col: str) -> DataFrame:
    """Separa os hórarios que a corrida começou em intervalos de tempo 
    em uma coluna nova ao DataFrame Spark alvo de acordo com a seguinte regra:
    0 - (0, 6)
    1 - (7, 12)
    2 - (13, 18)
    3 - (19, 23)

    Args:
        df (DataFrame): DataFrame Spark alvo
        timestamp_col (str): Nome da coluna que contem o timestamp do inicio da corrida

    Returns:
        DataFrame: Novo DataFrame Spark com a coluna de turnos
    """
    
    h = hour(col(timestamp_col))

    return df.withColumn(
        "hour_bucket",
        when(h.between(0, 6), 0)
        .when(h.between(7, 12), 1)
        .when(h.between(13, 18), 2)
        .when(h.between(19, 23), 3)
    )

def add_weekday(df: DataFrame, timestamp_col: str) -> DataFrame:
    """Adciona em qual dia da semana a corrida aconteceu 
    em uma coluna nova ao DataFrame Spark alvo

    Args:
        df (DataFrame): DataFrame Spark alvo
        timestamp_col (str): Nome da coluna que contem o timestamp do inicio da corrida

    Returns:
        DataFrame: Novo DataFrame Spark com a coluna de dia da semana da corrida
    """

    return df.withColumn(
        "weekday_num",
        dayofweek(col(timestamp_col))
    )

def enrich_trips(df: DataFrame) -> DataFrame:
    """Enriquece o DataFrame Spark alvo seguindo a ordem:
    1- Nova coluna para duração da corrida
    2- Nova coluna para total de taxas/impostos pagos
    3- Nova coluna para o turno da corrida
    4- Nova coluna para o dia da semana em que a corrida aconteceu

    Args:
        df (DataFrame): DataFrame Spark alvo

    Returns:
        DataFrame: Novo DataFrame Spark enriquecido
    """

    df = add_total_trip_duration(df, "pickup_datetime", "dropoff_datetime")
    df = add_total_taxes(df, "fate_amount", "total_amount")
    df = add_hour_bucket(df, "pickup_datetime")
    df = add_weekday(df, "pickup_datetime")
    
    return df