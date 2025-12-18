from pyspark.sql import SparkSession
import logging
import sys, os, traceback
from datetime import datetime

from extract.taxi_extract import extract_all_taxi_data
from transform.clean import final_clean
from transform.enrich import enrich_trips
from load.parquet_loader import write_metadata, write_parquet
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
        bronze_df = extract_all_taxi_data(spark)
        logger.info(f"Extraido {bronze_df.count()} entradas cruas")

        # Transformação
        silver_df = final_clean(bronze_df)
        gold_df = enrich_trips(silver_df)

        # Carregar
        write_metadata("data/gold", spark, gold_df.count(), None)
        write_parquet(gold_df, "data/gold", None, "overwrite")
        
    finally:
        spark.stop()
        logger.info("Sessão do Spark encerrada")

if __name__ == "__main__":
    run()