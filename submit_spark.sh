#!/bin/bash
# script para executar o pipeline spark com integração mongodb
spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  scripts/spark_mongodb_pipeline.py
