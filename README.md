## Basic Transformations


The purpose of this repo is to build data transformation applications.  The code contains ignored tests.  Please unignore these tests and make them pass.  

## Pre-requisites
Please make sure you have the following installed
* Java 8
* Scala 2.11
* Sbt 1.1.x
* Apache Spark 2.4 with ability to run spark-submit

## Setup Process
* Clone the repo
* Build: sbt package
* Test: sbt test

## Running Data Apps
* Package the project with
``` 
sbt package
``` 
* Sample data is available in the src/test/resource/data directory

### Wordcount
This applications will count the occurrences of a word within a text file. By default this app will read from the words.txt file and write to the target folder.  Pass in the input source path and output path directory to the spark-submit command below if you wish to use different files. 

```
spark-submit --class thoughtworks.wordcount.WordCount --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
```

Currently this application is a skeleton with ignored tests.  Please unignore the tests and build the wordcount application.

### Citibike multi-step pipeline
This application takes bike trip information and calculates the "as the crow flies" distance traveled for each trip.  
The application is run in two steps.
* First the data will be ingested from a sources and transformed to parquet format.
* Then the application will read the parquet files and apply the appropriate transformations.


* To ingest data from external source to datalake:
```
spark-submit --class thoughtworks.ingest.DailyDriver --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar $(INPUT_LOCATION) $(OUTPUT_LOCATION)
```

* To transform Citibike data:
```
spark-submit --class thoughtworks.citibike.CitibikeTransformer --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar $(INPUT_LOCATION) $(OUTPUT_LOCATION)
```

Currently this application is a skeleton with ignored tests.  Please unignore the tests and build the Citibike transformation application.

#### Tips
- For distance calculation, consider using [**Harvesine formula**](https://en.wikipedia.org/wiki/Haversine_formula) as an option.  
