#!/usr/bin/env bash

sbt package

$SPARK_HOME/bin/spark-submit \
  --class "Main" \
  --master local[4] \
  target/scala-2.12/hello-world_2.12-1.0.jar
