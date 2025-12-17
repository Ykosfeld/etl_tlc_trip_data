from pyspark.sql import SparkSession
import logging
import sys, os, traceback
from datetime import datetime

from extract.taxi_extract import extract_all_taxi_data
from utils.logging import setup_logging
from utils.spark import create_spark_session

def run():
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data/processed/orders", exist_ok=True)

    logger = setup_logging()
    logger.info("Starting Taxi ETL Pipeline")

    start_time = datetime.now()

    try:
        spark = create_spark_session()
        logger.info("Sessão do Spark criada")

        # Extração
        raw_df = extract_all_taxi_data(spark)
        logger.info(f"Extraido {raw_df.count()} entradas cruas")

    finally:
        spark.stop()
        logger.info("Sessão do Spark encerrada")

if __name__ == "__main__":
    run()