#!/usr/bin/env bash

if [ -z ${SPARK_HOME+x} ]; then echo "SPARK_HOME is not set"; exit 1;fi

sbt assembly 

$SPARK_HOME/bin/spark-submit \
  --class "Main" \
  --master local[4] \
  --conf spark.driver.GRAFANA_ENDPOINT="${GRAFANA_ENDPOINT}" \
  --conf spark.driver.GRAFANA_USER="${GRAFANA_USER}" \
  --conf spark.driver.GRAFANA_TOKEN="${GRAFANA_TOKEN}" \
  target/scala-2.12/hello-world-assembly-1.0.jar
