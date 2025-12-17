from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

logger = logging.getLogger(__name__)

def coalesce_timestamp(df: DataFrame, output_col: str, yellow_cab_col: str, green_cab_col: str) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        output_col (str): _description_
        yellow_cab_col (str): _description_
        green_cab_col (str): _description_

    Returns:
        DataFrame: _description_
    """
    return df.withColumn(
        output_col, 
        coalesce(col(yellow_cab_col), col(green_cab_col))
    ).drop(yellow_cab_col, green_cab_col)

def replace_value_with_null(df: DataFrame, column: str, invalid_value: int) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        column (str): _description_

    Returns:
        DataFrame: _description_
    """
    
    return df.withColumn(
        column,
        when(col(column) == invalid_value, None)
        .otherwise(col(column))
    ) 

def replace_null_with_value(df: DataFrame, column: str, correct_value: int | float) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        column (str): _description_
        correct_value (int): _description_

    Returns:
        DataFrame: _description_
    """
    
    return df.withColumn(
        column,
        when(col(column).isNull(), correct_value)
        .otherwise(col(column))
    )


def final_clean(df: DataFrame) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """

    time_clean_df = coalesce_timestamp(df, "pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime")
    time_clean_df = coalesce_timestamp(time_clean_df, "dropoff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime")

    clean_df = replace_value_with_null(time_clean_df, "payment_type", 5)
    clean_df = replace_value_with_null(clean_df, "RatecodeID", 99)

    clean_df = clean_df.drop("ehail_fee", "trip_type")

    clean_df = clean_df.withColumn(
        "Airport_fee",
        when(col("Airport_fee").isNull(), 0.0)
        .otherwise(col("Airport_fee"))
    )
    
    clean_df = replace_null_with_value("Airport_fee", 0.0)
    clean_df = replace_null_with_value("congestion_surcharge", 0.0)

    return clean_df