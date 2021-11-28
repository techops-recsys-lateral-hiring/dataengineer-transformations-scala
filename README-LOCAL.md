# Data transformations with Scala

This is a collection of jobs that are supposed to transform data.
These jobs are using _Spark_ to process larger volumes of data and are supposed to run on a _Spark_ cluster (via `spark-submit`).

## Pre-requisites
Please make sure you have the following installed
* Java 8
* Scala 2.12
* Sbt 1.1.x
* Apache Spark 2.4 with ability to run spark-submit

## Setup Process
* Clone the repo
* Package the project with `sbt package`
* Ensure that you're able to run the tests with `sbt test` (some are ignored)
* Sample data is available in the `src/test/resource/data` directory

## Jobs
There are two applications in this repo: Word Count, and Citibike.

Currently these exist as skeletons, and have some initial test cases which are defined but ignored.
For each application, please un-ignore the tests and implement the missing logic.

## Wordcount
A NLP model is dependent on a specific input file. This job is supposed to preprocess a given text file to produce this
input file for the NLP model (feature engineering). This job will count the occurrences of a word within the given text
file (corpus).

There is a dump of the data lake for this under `test/resources/data/words.txt` with a text file.

#### Input
Simple `*.txt` file containing text.

#### Output
A single `*.csv` file containing data similar to:
```csv
"word","count"
"a","3"
"an","5"
...
```

#### Run the Scala version of job
Please make sure to package the code before submitting the spark job
```
spark-submit --class thoughtworks.wordcount.WordCount --master local target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar
```


## Citibike
For analytics purposes the BI department of a bike share company would like to present dashboards, displaying the
distance each bike was driven. There is a `*.csv` file that contains historical data of previous bike rides. This input
file needs to be processed in multiple steps. There is a pipeline running these jobs.

![citibike pipeline](docs/citibike.png)

There is a dump of the datalake for this under `test/resources/citibike/citibike.csv` with historical data.

### Ingest
Reads a `*.csv` file and transforms it to parquet format. The column names will be sanitized (whitespaces replaced).

##### Input
Historical bike ride `*.csv` file:
```csv
"tripduration","starttime","stoptime","start station id","start station name","start station latitude",...
364,"2017-07-01 00:00:00","2017-07-01 00:06:05",539,"Metropolitan Ave & Bedford Ave",40.71534825,...
...
```

##### Output
`*.parquet` files containing the same content
```csv
"tripduration","starttime","stoptime","start_station_id","start_station_name","start_station_latitude",...
364,"2017-07-01 00:00:00","2017-07-01 00:06:05",539,"Metropolitan Ave & Bedford Ave",40.71534825,...
...
```

##### Run the Scala version of job
Please make sure to package the code before submitting the spark job
```
spark-submit --class thoughtworks.ingest.DailyDriver --master local target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar $(INPUT_LOCATION) $(OUTPUT_LOCATION)
```

### Distance calculation
This job takes bike trip information and calculates the "as the crow flies" distance traveled for each trip.
It reads the previously ingested data parquet files.

Hint: For distance calculation, consider using [**Harvesine formula**](https://en.wikipedia.org/wiki/Haversine_formula) as an option.

##### Input
Historical bike ride `*.parquet` files
```csv
"tripduration",...
364,...
...
```

##### Outputs
`*.parquet` files containing historical data with distance column containing the calculated distance.
```csv
"tripduration",...,"distance"
364,...,1.34
...
```

##### Run the Scala version of job
Please make sure to package the code before submitting the spark job
```
spark-submit --class thoughtworks.citibike.CitibikeTransformer --master local target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar $(INPUT_LOCATION) $(OUTPUT_LOCATION)
```
