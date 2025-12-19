from pyspark.sql import SparkSession
import logging
import sys, os, traceback
from datetime import datetime

from extract.taxi_extract import extract_taxi_data
from transform.clean import final_clean
from transform.enrich import enrich_trips
from transform.join import join_dataframes
from load.parquet_loader import write_metadata, write_parquet
from utils.logging import setup_logging
from utils.spark import create_spark_session
from config.schemas import yellow_taxi_schema, green_taxi_schema

def run():
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data/gold", exist_ok=True)

    logger = setup_logging()
    logger.info("Starting Taxi ETL Pipeline")

    start_time = datetime.now()

    try:
        spark = create_spark_session()
        logger.info("Sessão do Spark criada")

        # Extração
        spark.sparkContext.setJobDescription("Bronze | Lendo corridas de taxi cruas")
        
        bronze_yellow_df = extract_taxi_data(spark, "data/raw/yellow_tripdata_2025-*.parquet", yellow_taxi_schema)
        bronze_green_df = extract_taxi_data(spark, "data/raw/green_tripdata_2025-*.parquet", green_taxi_schema)
        logger.info(f"Extraido {bronze_yellow_df.count() + bronze_green_df.count()} entradas cruas")

        # Transformação
        spark.sparkContext.setJobDescription("Silver | Limpando e enriquecendo corridas de taxi")
        
        silver_yellow_df = final_clean(bronze_yellow_df)
        silver_green_df = final_clean(bronze_green_df)
        
        gold_yellow_df = enrich_trips(silver_yellow_df, "yellow")
        gold_green_df = enrich_trips(silver_green_df, "green")

        gold_final_df = join_dataframes(gold_yellow_df, gold_green_df)
        logger.info(f"{gold_final_df.count()} entradas limpas e enriquecidas")

        # Carregar
        spark.sparkContext.setJobDescription("Gold | Salvando corridas de taxi limpas e enriquecidas")
        
        output_path = "data/gold"
        write_metadata("data/metadata", spark, gold_final_df.count(), ["taxi_type"])
        write_parquet(gold_final_df, output_path, ["taxi_type"], "overwrite")

        runtime = (datetime.now() - start_time).total_seconds()
        logger.info(f"A pipeline foi completada com sucesso em {runtime:.2f} segunds")

    except Exception as e:
        logger.error(f"A pipeline falhou: {str(e)}")
        logger.error(traceback.format_exc())
    
    finally:
        spark.stop()
        logger.info("Sessão do Spark encerrada")

if __name__ == "__main__":
    run()