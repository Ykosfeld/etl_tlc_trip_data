from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

yellow_taxi_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
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
    StructField("VendorID", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", LongType(), True),
    StructField("trip_type", LongType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])

metadata_schema = StructType([
        StructField("execution_date", StringType(), False),
        StructField("spark_version", StringType(), False),
        StructField("row_count", IntegerType(), False),
        StructField("partition_columns", ArrayType(StringType()), True),
])

VENDOR_ID = "VendorID"
TPEP_PICKUP_DATETIME = "tpep_pickup_datetime"
TPEP_DROPOFF_DATETIME = "tpep_dropoff_datetime"
LPEP_PICKUP_DATETIME = "lpep_pickup_datetime"
LPEP_DROPOFF_DATETIME = "lpep_dropoff_datetime"
PASSENGER_COUNT = "passenger_count"
TRIP_DISTANCE = "trip_distance"
RATECODE_ID = "RatecodeID"
STORE_AND_FWD_FLAG = "store_and_fwd_flag"
PULOCATION_ID = "PULocationID"
DOLOCATION_ID = "DOLocationID"
PAYMENT_TYPE = "payment_type"
FARE_AMOUNT = "fare_amount"
EXTRA = "extra"
MTA_TAX = "mta_tax"
TIP_AMOUNT = "tip_amount"
TOLLS_AMOUNT = "tolls_amount"
IMPROVEMENT_SURCHARGE = "improvement_surcharge"
TOTAL_AMOUNT = "total_amount"
CONGESTION_SURCHARGE = "congestion_surcharge"
AIRPORT_FEE = "Airport_fee"
CBD_CONGESTION_FEE = "cbd_congestion_fee"
EHAIL_FEE = "ehail_fee"
TRIP_TYPE = "trip_type"