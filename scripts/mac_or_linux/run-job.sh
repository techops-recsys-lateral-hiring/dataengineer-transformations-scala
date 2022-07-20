#!/bin/bash

set -euo pipefail

sbt clean package

scalaVersion=2.12
originalJob="${JOB:-""}"
jobName=$(echo "${originalJob}" | awk '{ print tolower($1) }')
JAR=target/scala-"${scalaVersion}"/tw-pipeline_"${scalaVersion}"-0.1.0-SNAPSHOT.jar

if [[ "${jobName}" == "citibike_ingest" ]]; then
    JOB=thoughtworks.ingest.DailyDriver
    INPUT_FILE_PATH="./src/main/resources/data/citibike.csv"
    OUTPUT_PATH="./output_int"
elif [[ "${jobName}" == "citibike_distance_calculation" ]]; then
    JOB=thoughtworks.citibike.CitibikeTransformer
    INPUT_FILE_PATH="./output_int"
    OUTPUT_PATH="./output"
elif [[ "${jobName}" == "wordcount" ]]; then
    JOB=thoughtworks.wordcount.WordCount
    INPUT_FILE_PATH="./src/main/resources/data/words.txt"
    OUTPUT_PATH="./output"
else
  echo "Job name provided was : ${originalJob} : failed"
  echo "Job name deduced was : ${jobName} : failed"
  echo "Please enter a valid job name (citibike_ingest, citibike_distance_calculation or wordcount)"
  exit 1
fi

rm -rf $OUTPUT_PATH
echo "Executing Job : ${originalJob}"
echo "Jar : ${JAR}"
echo "Input : ${INPUT_FILE_PATH}"
echo "Output : ${OUTPUT_PATH}"


spark-submit \
    --master local \
    --class $JOB \
    $JAR \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH



