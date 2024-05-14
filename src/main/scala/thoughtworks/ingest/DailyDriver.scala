// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package thoughtworks.ingest

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DailyDriver {
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]) {
    log.setLevel(Level.INFO)
    val (inputSource: String, outputPath: String) = getInputAndOutputPaths(args)

    val spark = SparkSession.builder.appName("Skinny Pipeline: Ingest").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    run(spark, inputSource, outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  private def getInputAndOutputPaths(args: Array[String]) = {
    if (args.length < 2) {
      log.warn("Input source and output path are required")
      System.exit(1)
    }

    val ingestPath = args(0)
    val transformationPath = args(1)
    (ingestPath, transformationPath)
  }

  def run(spark: SparkSession, inputSource: String, outputPath: String): Unit = {
    val inputDataFrame = spark.read
      .format("org.apache.spark.csv")
      .option("header", value = true)
      .csv(inputSource)

    formatColumnHeaders(inputDataFrame)
      .write
      .parquet(outputPath)
  }

  private def formatColumnHeaders(dataFrame: DataFrame): DataFrame = {
    var df = dataFrame
    for (column <- df.columns) {
      df = df.withColumnRenamed(column, column.replaceAll("\\s", "_"))
    }
    df.printSchema()
    df
  }
}
