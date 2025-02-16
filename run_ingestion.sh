#!/bin/bash
#spark-submit --jars /opt/spark/jars/delta-core_2.12-2.4.0.jar /app/ingest.py
spark-submit --packages io.delta:delta-spark_2.12:3.3.0 /app/ingest.py