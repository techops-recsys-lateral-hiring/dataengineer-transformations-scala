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

package thoughtworks.wordcount

import java.time.LocalDateTime

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = if(!args.isEmpty) args(0) else "./src/test/resources/data/words.txt"
    val outputPath = if(args.length > 1) args(1) else "./target/test-" + LocalDateTime.now()

    run(spark, inputPath, outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    log.info("Reading text file from: " + inputPath)
    log.info("Writing csv to directory: " + outputPath)

    import spark.implicits._
    import WordCountUtils._
    spark
      .read
      .text(inputPath)
      .as[String]
      .splitWords(spark)
      .countByWord(spark)
      .write
      .csv(outputPath)
  }
}
