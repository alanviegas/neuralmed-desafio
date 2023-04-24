# The base image with pyspark
FROM apache/spark-py:v3.2.1

COPY . .

USER root

COPY ./src/shared/gcs-connector-hadoop2-latest.jar /opt/spark/jars/gcs-connector-hadoop2-latest.jar
COPY ./src/shared/google-api-client-2.2.0.jar /opt/spark/jars/google-api-client-2.2.0.jar

# python requirements
RUN pip install -r requirements.txt

RUN pip install ./dist/neuralmed-desafio-0.0.1.tar.gz


