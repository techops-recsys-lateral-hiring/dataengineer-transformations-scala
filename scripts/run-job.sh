#!/bin/bash

set -e

err() {
  echo "$1" >&2
}

if [[ -z ${INPUT_FILE_PATH} ]];
then
  err "INPUT_FILE_PATH environment variable not set"
else
  echo "INPUT_FILE_PATH is ${INPUT_FILE_PATH}"
fi

if [[ -z ${OUTPUT_PATH} ]];
then
  err "OUTPUT_PATH environment variable not set"
else
  echo "OUTPUT_PATH is ${OUTPUT_PATH}"
fi

if [[ -z "${OUTPUT_PATH}" || -z "${INPUT_FILE_PATH}" ]];
then
  exit 1
fi

JAR=target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar

rm -rf $OUTPUT_PATH

sbt clean package

spark-submit \
    --master local \
    --class $JOB \
    $JAR \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH

