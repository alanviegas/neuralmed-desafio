import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # espera receber o nome da tabela a ser processada como parametro
    if len(sys.argv) != 2:
        print("Argumento necessario: <table_name>")
        print(sys.argv)
        print(len(sys.argv))
        exit(-1)

    table_name = sys.argv[1:][0]

    conf = (
        SparkConf()
        .setAppName("load neuralmed data files")
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.repl.eagerEval.enabled", "true")
        .set("viewsEnabled", "true")
        .set("materializationDataset", "raw_neuralmed")
        .set("spark.submit.pyFiles", "/dist/neuralmed-desafio-0.0.1.tar.gz")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    spark._jsc.hadoopConfiguration().set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    spark._jsc.hadoopConfiguration().set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    spark._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    # This is required if you are using service account and set true,
    spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    )

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j  # type: ignore

    LOGGER = log4jLogger.LogManager.getLogger(__name__)  # noqa
    LOGGER.info("load neuralmed data files")

    LOGGER.info(f"Parametros: {table_name}")

    from neuralmed_raw.pipeline import main

    main(table_name, spark)
