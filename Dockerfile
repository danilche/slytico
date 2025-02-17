FROM bitnami/spark:latest

USER root

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY jobs/. /app/jobs/.
COPY scripts/. /app/scripts/.
COPY tests/. /app/tests/.
COPY utils/. /app/utils/.
COPY config/. /app/config/.

WORKDIR /app

RUN chmod +x /app/scripts/pipeline.sh

RUN mkdir -p /app/logs && chmod 777 /app/logs
RUN mkdir -p /app/results && chmod 777 /app/results
RUN mkdir -p /root/.ivy2 && chmod -R 777 /root/.ivy2

RUN pytest tests/

ENTRYPOINT ["/bin/bash"]