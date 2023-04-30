-- criação das external tables apontando para os arqivos parquet's

CREATE OR REPLACE EXTERNAL TABLE `neuralmed_raw.Exam`
WITH PARTITION COLUMNS
OPTIONS(
	format="PARQUET",
	hive_partition_uri_prefix="gs://neuralmed-tst/neuralmed_raw/Exam/",
	uris=["gs://neuralmed-tst/neuralmed_raw/Exam/*"],
    require_hive_partition_filter = false
);

CREATE OR REPLACE EXTERNAL TABLE `neuralmed_raw.Label`
WITH PARTITION COLUMNS
OPTIONS(
	format="PARQUET",
	hive_partition_uri_prefix="gs://neuralmed-tst/neuralmed_raw/Label/",
	uris=["gs://neuralmed-tst/neuralmed_raw/Label/*"],
    require_hive_partition_filter = false
);

CREATE OR REPLACE EXTERNAL TABLE `neuralmed_raw.Medical_Report`
WITH PARTITION COLUMNS
OPTIONS(
	format="PARQUET",
	hive_partition_uri_prefix="gs://neuralmed-tst/neuralmed_raw/Medical_Report/",
	uris=["gs://neuralmed-tst/neuralmed_raw/Medical_Report/*"],
    require_hive_partition_filter = false
);
