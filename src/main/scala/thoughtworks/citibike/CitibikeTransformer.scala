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

package thoughtworks.citibike

import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.SparkSession

object CitibikeTransformer {
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val (ingestPath: String, transformationPath: String) = getInputAndOutputPaths(args)

    val spark = SparkSession.builder.appName("Citibike Transformer").getOrCreate()
    log.info("Citibike Transformer Application Initialized: " + spark.sparkContext.appName)

    run(spark, ingestPath, transformationPath)

    log.info("Citibike Application Done: " + spark.sparkContext.appName)
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

  def run(sparkSession: SparkSession,
          ingestPath: String,
          outputPath: String): Unit = {

    import CitibikeTransformerUtils._

    val df = sparkSession.read
      .parquet(ingestPath)
      .computeDistances(sparkSession)

    df.show(false)

    df
      .write
      .parquet(outputPath)
  }
}
