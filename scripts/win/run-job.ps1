sbt clean package

$JOB = [System.Environment]::GetEnvironmentVariable('JOB')
$jobName = $JOB.ToLower()
$scalaVersion = 2.12
$JAR = "target\scala-$scalaVersion\tw-pipeline_$scalaVersion-0.1.0-SNAPSHOT.jar"

switch ($jobName)
{

    citibike_ingest {
        $JOB=thoughtworks.ingest.DailyDriver
        $INPUT_FILE_PATH="./src/main/resources/data/citibike.csv"
        $OUTPUT_PATH="./output_int"
        Break
    }
    citibike_distance_calculation {
        $JOB=thoughtworks.citibike.CitibikeTransformer
        $INPUT_FILE_PATH="./output_int"
        $OUTPUT_PATH="./output"
        Break
    }
    wordcount {
        $JOB=thoughtworks.wordcount.WordCount
        $INPUT_FILE_PATH="./src/main/resources/data/words.txt"
        $OUTPUT_PATH="./output"
        Break
    }
    default {
        Write-Host "Job name provided was : $JOB : failed"
        Write-Host "Job name deduced was : $jobName : failed"
        Write-Host "Please enter a valid job name (citibike_ingest, citibike_distance_calculation or wordcount)"
        exit 1
        Break
    }


}


rm -rf $OUTPUT_PATH

Write-Output "Executing Job : $originalJob"
Write-Output "Jar : $JAR"
Write-Output "Input : $INPUT_FILE_PATH"
Write-Output "Output : $OUTPUT_PATH"


spark-submit --master local --class $JOB $JAR $INPUT_FILE_PATH $OUTPUT_PATH

