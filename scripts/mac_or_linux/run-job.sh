#!/bin/bash

set -euo pipefail

./gradlew clean build

originalJob="${JOB:-""}"
jobName=$(echo "${originalJob}" | awk '{ print tolower($1) }')
JAR=build/libs/dataengineer.jar

if [[ "${jobName}" == "citibike_ingest" ]]; then
    JOB=com.thoughtworks.de.ingest.DailyDriver
    INPUT_FILE_PATH="./resources/citibike/citibike.csv"
    OUTPUT_PATH="./output_int"
elif [[ "${jobName}" == "citibike_distance_calculation" ]]; then
    JOB=com.thoughtworks.de.citibike.CitibikeTransformer
    INPUT_FILE_PATH="./output_int"
    OUTPUT_PATH="./output"
elif [[ "${jobName}" == "wordcount" ]]; then
    JOB=com.thoughtworks.de.wordcount.WordCount
    INPUT_FILE_PATH="./resources/word_count/words.txt"
    OUTPUT_PATH="./output"
else
  echo "Job name provided was : ${originalJob} : failed"
  echo "Job name deduced was : ${jobName} : failed"
  echo "Please enter a valid job name (citibike_ingest, citibike_distance_calculation or wordcount)"
  exit 1
fi

rm -rf $OUTPUT_PATH


spark-submit \
    --master local \
    --class $JOB \
    $JAR \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH



