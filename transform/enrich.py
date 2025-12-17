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

def add_total_trip_duration(df: DataFrame, pickup_col: str, dropoff_col: str) -:
    return df.withColumn(
        "trip_duration",
        expr(f"timestampdiff(SECOND, {pickup_col}, {dropoff_col})")
    )

def add_total_taxes(df: DataFrame, fare_amount_col: str, total_amount_col: str):
    return df.withColumn(
        "total_taxes",
        col(total_amount_col) - col(fare_amount_col)
    )

def add_hour_bucket(df: DataFrame, timestamp_col: str):
    h = hour(col(timestamp_col))

    return df.withColumn(
        "hour_bucket",
        when(h.between(0, 6), 0)
        .when(h.between(7, 12), 1)
        .when(h.between(13, 18), 2)
        .when(h.between(19, 23), 3)
    )

def add_weekday(df: DataFrame, timestamp_col: str):
    return df.withColumn(
        "weekday_num",
        dayofweek(col(timestamp_col))
    )

def enrich_trips(df: DataFrame):
    df = add_total_trip_duration(df, "pickup_datetime", "dropoff_datetime")
    df = add_total_taxes(df, "fate_amount", "total_amount")
    df = add_hour_bucket(df, "pickup_datetime")
    df = add_weekday(df, "pickup_datetime")
    
    return df