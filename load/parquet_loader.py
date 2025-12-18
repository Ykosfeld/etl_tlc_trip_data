from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

import json
from pathlib import Path
import datetime

logger = logging.getLogger(__name__)

def write_metadata(
    output_path: str,
    spark,
    row_count: int,
    partition_cols: list[str] | None = None
) -> str:
    execution_date = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
    metadata = {
        "execution_date": execution_date,
        "spark_version": spark.version,
        "row_count": row_count,
        "partition_columns": partition_cols or []
    }   

    metadata_dir = Path(output_path).resolve()
    metadata_dir.mkdir(parents=True, exist_ok=True)

    file_path = metadata_dir / f"run_{execution_date}.json"
    
    with open(file_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.warning(f"[METADATA] Criado em: {file_path.resolve()}")

    return str(file_path)


def write_parquet(
    df: DataFrame,
    output_path: str,
    partition_cols: list[str] = None,
    mode: str = "overwrite"
):
    """Salva o DataFrame Spark no tipo 'parquet', podendo ser particionado ou não de acordo com uma lista de colunas

    Args:
        df (DataFrame): DataFrame Spark alvo
        output_path (str): Diretório onde o DataFrame será salvo em formato Parquet
        partition_cols (list[str], optional): Colunas que serão criterio de particionamento. Defaults to None.
        mode (str, optional): Parametro a ser utilizado na função '.mode' do DataFrame Spark. Defaults to "overwrite".
    """

    logger.info(f"Serão gravados {df.count()} linhas")
    logger.info(f"O DataFrame será gravado com o seguinte esquema: \n{df.schema}")
    
    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.parquet(output_path)

