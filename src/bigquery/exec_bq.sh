#!/bin/bash

SQL_FILE=$1
SQL_PATH="/src/bigquery/neuralmed_prep/$SQL_FILE"

gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
gcloud config set project $GCP_PROJECT_ID
gcloud config set disable_prompts true

echo "Running $SQL_PATH"
bq query \
--use_legacy_sql=false \
--parameter='GCP_PROJECT_ID::'$GCP_PROJECT_ID'' \
"`cat $SQL_PATH`"
