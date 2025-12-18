from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

import json
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

def write_metadata(
    output_path: str,
    spark, 
    row_count: int, 
    partition_cols: list[str] = None
):
    """Cria um arquivo json de meta dados sobre a execução do job 
    com as seguintes informações: dia de execução, versão do Spark, número de linhas salvas, colunas para criterio de particionamento

    Args:
        output_path (str): Caminho de onde salvar o arquivo de meta dados
        spark (_type_): Sessão do Spark
        row_count (int): Número de linhas do DataFrame Spark alvo
        partition_cols (list[str], optional): Colunas que serão criterio de particionamento. Defaults to None.
    """
    metadata = {
        "execution_date": datetime.utcnow().isoformat(),
        "spark_version": spark.version,
        "row_count": row_count,
        "partition_columns": partition_cols 
    }

    metadata_dir = Path(output_path) / "_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    file_path = metadata_dir / f"run_{metadata['execution_date']}.json"

    with open(file_path, "w") as f:
        json.dump(metadata, f, indent=2)


def write_parquet(
    df: DataFrame,
    output_path: str,
    partition_cols: list[str] = None,
    mode: str = "overwrite"
):
    """Salva o DataFrame Spark no tipo 'parquet', podendo ser particionado ou não de acordo com uma lista de colunas

    Args:
        df (DataFrame): DataFrame Spark alvo
        output_path (str): Caminho de onde salvar o DataFrame Spark
        partition_cols (list[str], optional): Colunas que serão criterio de particionamento. Defaults to None.
        mode (str, optional): Parametro a ser utilizado na função '.mode' do DataFrame Spark. Defaults to "overwrite".
    """
    
    logger.info(f"Serão gravados {df.count()} linhas")
    logger.info(f"O DataFrame será gravado com o seguinte esquema: \n{df.schema}")
    
    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.parquet(output_path)

