FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y curl

RUN pip install delta-spark==3.3.0 pandas

# RUN mkdir -p /opt/spark/jars && \
#     curl -L -o /opt/spark/jars/delta-core_2.12-2.4.0.jar \
#     https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
#     curl -L -o /opt/spark/jars/delta-storage_2.12-2.4.0.jar \
#     https://repo1.maven.org/maven2/io/delta/delta-storage_2.12/2.4.0/delta-storage_2.12-2.4.0.jar

# ENV SPARK_CLASSPATH="/opt/spark/jars/delta-core_2.12-2.4.0.jar:/opt/spark/jars/delta-storage_2.12-2.4.0.jar"

COPY jobz/ingest.py /app/ingest.py
COPY jobz/transform.py /app/transform.py
COPY jobz/test.py /app/test.py
COPY run_ingestion.sh /app/run_ingestion.sh
COPY run_processing.sh /app/run_processing.sh
COPY run_test.sh /app/run_test.sh

WORKDIR /app

RUN chmod +x /app/run_ingestion.sh /app/run_processing.sh
RUN mkdir -p /app/logs && chmod 777 /app/logs
RUN mkdir -p /root/.ivy2 && chmod -R 777 /root/.ivy2

ENTRYPOINT ["/bin/bash"]