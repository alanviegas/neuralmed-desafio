import json
import logging
from typing import Any

from pyspark.sql import functions as F

from neuralmed_raw.utils.constants import CONFIG_SCHEMAS_FILE, DATE_LOAD, RAW_FILES_PATH
from neuralmed_raw.utils.helpers import parse_json


def main(table_name: str, spark: Any):
    """
    todo
    """

    with open(CONFIG_SCHEMAS_FILE, "r") as j:  # type: ignore
        config_schemas = json.loads(j.read())

    source_columns = []
    target_columns = []
    for k, v in config_schemas[table_name]["columns"].items():
        source_columns.append(k)
        target_columns.append(v)

    logging.info("read json files")
    rawJSON = spark.read.json(config_schemas[table_name]["path_files"], multiLine="true")

    if table_name == "Label":
        rawDF = rawJSON.withColumn("labels", F.explode("content.labels"))
        rawDF = rawDF.select("content.id", "labels.*")
    else:
        rawDF = rawJSON.select(*source_columns)

    logging.info("parsing data files")
    parsedRDD = rawDF.rdd.map(lambda x: parse_json(x, target_columns))

    columns_list = list(dict(target_columns).keys())

    parsedDF = parsedRDD.toDF(columns_list)

    parsedFinalDF = parsedDF.withColumn("dt_load", F.lit(DATE_LOAD))

    parsedFinalDF.printSchema()
    parsedFinalDF.show()

    logging.info("saving on bucket raw")
    parsedFinalDF.write.partitionBy("dt_load").mode("overwrite").format("parquet").option(
        "compression", "snappy"
    ).save(f"{RAW_FILES_PATH}/{table_name}")

    logging.info("load neuralmed data files finished")
