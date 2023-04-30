import json
import logging
from typing import Any

from pyspark.sql import functions as F

from neuralmed_raw.utils.constants import CONFIG_SCHEMAS_FILE, DATE_LOAD, RAW_FILES_PATH
from neuralmed_raw.utils.helpers import parse_json


def main(table_name: str, spark: Any):
    """
    Função principal que executa pipeline - leitura dos arquivos de dados, faz o parse dos dados 
    (converte datatypes) de acordo com os parâmetros do arquivo de configuração,e 
    carrrega no Storage como arquivo parquet particionado
    """

    # arquivo de configuração com os schemas das tabelas
    with open(CONFIG_SCHEMAS_FILE, "r") as j:  # type: ignore
        config_schemas = json.loads(j.read())

    source_columns = []
    target_columns = []
    for k, v in config_schemas[table_name]["columns"].items():
        source_columns.append(k)
        target_columns.append(v)

    logging.info("read json files")
    # carrega os json para para um dataframe spark
    rawJSON = spark.read.json(config_schemas[table_name]["path_files"], multiLine="true")
    
    # se a tabela for "Label" a leitura dos jsons é um pouco diferente, por que os dados estão aninhados
    if table_name == "Label":
        rawDF = rawJSON.withColumn("labels", F.explode("content.labels"))
        rawDF = rawDF.select("content.id", "labels.*")
    else:
        rawDF = rawJSON.select(*source_columns)

    logging.info("parsing data files")
    parsedRDD = rawDF.rdd.map(lambda x: parse_json(x, target_columns))

    columns_list = list(dict(target_columns).keys())
    
    # Converte os dados tratados para um outro dataframe 
    parsedDF = parsedRDD.toDF(columns_list)

    # inclui um data de carga
    parsedFinalDF = parsedDF.withColumn("dt_load", F.lit(DATE_LOAD))

    parsedFinalDF.printSchema()
    parsedFinalDF.show()

    logging.info("saving on bucket raw")
    # salva no storage particionado pelo data de carga em formato parquet e compressão snappy 
    parsedFinalDF.write.partitionBy("dt_load").mode("overwrite").format("parquet").option(
        "compression", "snappy"
    ).save(f"{RAW_FILES_PATH}/{table_name}")

    logging.info("load neuralmed data files finished")
