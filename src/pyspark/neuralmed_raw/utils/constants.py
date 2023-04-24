import os
from datetime import datetime

import pytz

CONFIG_SCHEMAS_FILE = os.getenv("CONFIG_SCHEMAS_FILE")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJETCT_ID = os.getenv("GCP_PROJECT_ID")
RAW_BUCKET = os.getenv("RAW_BUCKET")

RAW_FILES_PATH = f"gs://{RAW_BUCKET}/neuralmed_raw"

DATE_LOAD_RAW = datetime.now(pytz.timezone("America/Sao_Paulo"))
DATE_LOAD: str = DATE_LOAD_RAW.strftime("%Y-%m-%d")
