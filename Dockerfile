FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y curl

RUN pip install delta-spark==3.3.0 pandas

COPY jobs/. /app/jobs/.
COPY scripts/. /app/scripts/.
COPY utils/. /app/utils/.

WORKDIR /app

RUN chmod +x /app/scripts

RUN mkdir -p /app/logs && chmod 777 /app/logs
RUN mkdir -p /app/results && chmod 777 /app/results
RUN mkdir -p /root/.ivy2 && chmod -R 777 /root/.ivy2

ENTRYPOINT ["/bin/bash"]