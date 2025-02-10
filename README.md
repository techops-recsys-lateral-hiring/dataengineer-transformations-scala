# Data transformations with Scala

This is a collection of jobs that are supposed to transform data.
These jobs are using _Spark_ to process larger volumes of data and are supposed to run on a _Spark_ cluster (
via `spark-submit`).

## Gearing Up for the Pairing Session

**✅ Goals**

1. **Get a working environment**  
   Either local ([local](#local-setup), or using [gitpod](#gitpod-setup))
2. **Get a high-level understanding of the code and test dataset structure**
3. Have your preferred text editor or IDE setup and ready to go.

**❌ Non-Goals**

- solving the exercises / writing code
  > ⚠️ The exercises will be given at the time of interview, and solved by pairing with the interviewer.

## Pre-requisites

Please make sure you have the following installed

* Java 11
* Scala 2.12.16
* Sbt 1.7.x
* Apache Spark 3.3 with ability to run spark-submit

## Local Setup Process

* Clone the repo
* Package the project with `sbt package`
* Ensure that you're able to run the tests with `sbt test` (some are ignored)
* Sample data is available in the `src/test/resource/data` directory

> 💡 If you don't manage to run the local setup or you have restrictions to install software in your laptop, use
> the [gitpod](#gitpod-setup) one

### Cloud Development Setup

You can quickly set up the development environment using either Gitpod or Codeanywhere:

#### 🌐 Gitpod

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/techops-recsys-lateral-hiring/dataengineer-transformations-scala)

#### 🌍 Codeanywhere
[![Open in Codeanywhere](https://codeanywhere.com/img/open-in-codeanywhere-btn.svg)](https://app.codeanywhere.com/#https://github.com/techops-recsys-lateral-hiring/dataengineer-transformations-scala)

#### Setup Instructions
- An initialization script is preconfigured and takes approximately 3 minutes to complete.
- When opening the repository in a new workspace, wait for all required packages to install.
- After setup is complete:
   - Select Poetry’s environment by clicking the thumbs-up icon.
    - Navigate to the Testing tab and hit the refresh icon to discover available tests.

#### Using Gitpod with VS Code
You can either use Gitpod’s web interface or configure SSH access to work with VS Code on your local machine.
Follow this [guide](https://www.gitpod.io/docs/references/ides-and-editors/vscode#connecting-to-vs-code-desktop)  for setup instructions.

#### Important Reminder
> Before your interview, stop the VM and restart it to ensure a fresh environment.

### Verify setup

> All of the following commands should be running successfully

#### Run all tests

```bash
sbt test
```

#### Run specific tests class

```bash
sbt "test:testOnly *MySuite"
```

#### Run style checks

```bash
sbt scalastyle
```

---
# STOP HERE: Do not code before the interview begins.
---


---
# STOP HERE: Do not code before the interview begins.
---

## Jobs

There are two applications in this repo: Word Count, and Citibike.

Currently these exist as skeletons, and have some initial test cases which are defined but ignored. For each
application, please un-ignore the tests and implement the missing logic.

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

#### Run the job

```bash
 spark-submit --master local --class thoughtworks.wordcount.WordCount \
    target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar \
    "./src/main/resources/data/words.txt" \
    ./output
```

### Citibike

For analytics purposes the BI department of a bike share company would like to present dashboards, displaying the
distance each bike was driven. There is a `*.csv` file that contains historical data of previous bike rides. This input
file needs to be processed in multiple steps. There is a pipeline running these jobs.

![citibike pipeline](docs/citibike.png)

There is a dump of the datalake for this under `/src/test/resources/data/citibike.csv` with historical data.

#### Ingest

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

##### Run the job

```bash
spark-submit --master local --class thoughtworks.ingest.DailyDriver \
    target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar \
    "./src/main/resources/data/citibike.csv" \
    "./output_int"
```

#### Distance calculation

This job takes bike trip information and calculates the "as the crow flies" distance traveled for each trip. It reads
the previously ingested data parquet files.

Hint:

- For distance calculation, consider using [**Harvesine formula**](https://en.wikipedia.org/wiki/Haversine_formula) as
  an option.

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

##### Run the job

```bash
 spark-submit --master local --class thoughtworks.citibike.CitibikeTransformer \
    target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar \
    "./output_int" \
    ./output
```