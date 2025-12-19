from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

logger = logging.getLogger(__name__)

def join_dataframes(df1: DataFrame, df2: DataFrame) -> DataFrame:
    return df1.unionByName(df2, allowMissingColumns=True).drop("ehail_fee", "trip_type")

