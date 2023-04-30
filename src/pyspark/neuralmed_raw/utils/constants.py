import os
from datetime import datetime

import pytz

# arquivo com os schemas das tabelas
CONFIG_SCHEMAS_FILE = os.getenv("CONFIG_SCHEMAS_FILE")
# credencial do GCP
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# projeto no GCP
GCP_PROJETCT_ID = os.getenv("GCP_PROJECT_ID")
# bucket no GCP
RAW_BUCKET = os.getenv("RAW_BUCKET")
# local onde serão armazenados os arquivos parquet
RAW_FILES_PATH = f"gs://{RAW_BUCKET}/neuralmed_raw"
# data de carga
DATE_LOAD_RAW = datetime.now(pytz.timezone("America/Sao_Paulo"))
DATE_LOAD: str = DATE_LOAD_RAW.strftime("%Y-%m-%d")
