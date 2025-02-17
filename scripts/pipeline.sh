#!/bin/bash
spark-submit --packages io.delta:delta-spark_2.12:3.3.0  /app/jobs/ingest.py
sleep 3
spark-submit --packages io.delta:delta-spark_2.12:3.3.0  /app/jobs/transform.py
sleep 3
spark-submit --packages io.delta:delta-spark_2.12:3.3.0  /app/jobs/calculate.py