#!/bin/bash

set -e

OUTPUT_PATH="./output"
JAR=target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar

rm -rf $OUTPUT_PATH

sbt clean package

spark-submit \
    --master local \
    --class $JOB \
    $JAR \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH

