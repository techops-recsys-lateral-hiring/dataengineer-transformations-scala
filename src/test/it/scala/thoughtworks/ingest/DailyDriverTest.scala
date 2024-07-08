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
package it.scala.thoughtworks.ingest

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import thoughtworks.DefaultFeatureSpecWithSpark
import thoughtworks.ingest.DailyDriver

import java.nio.file.Files


class DailyDriverTest extends AnyFunSuite with DefaultFeatureSpecWithSpark {

  test("input data should be in expected format") {
    val rootDirectory = Files.createTempDirectory(this.getClass.getName)
    val inputCsv = rootDirectory.toUri.toString + "input.csv"
    val outputDirectory = rootDirectory.resolve("output")
    val csvRows = Seq(
      Row(3,1,4,3),
      Row(1,5,2,4)
    )
    val csvSchema = List(
      StructField("first_field", IntegerType, nullable = true),
      StructField("field with space", IntegerType, nullable = true),
      StructField(" fieldWithOuterSpaces ", IntegerType, nullable = true),
      StructField("field_With0ut_Spaces", IntegerType, nullable = true),
    )
    val inputDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(csvRows),
      StructType(csvSchema))
    inputDataFrame.write
      .option("header",value = true)
      .option("quoteAll", value = true)
      .option("ignoreLeadingWhiteSpace", value = false)
      .option("ignoreTrailingWhiteSpace", value = false)
      .csv(inputCsv)

    DailyDriver.run(spark, inputCsv, outputDirectory.toUri.toString)
    val parquetDirectory = spark.read.parquet(outputDirectory.toUri.toString)
    parquetDirectory.count should be (2)
    parquetDirectory.columns should be (Array("first_field", "field_with_space", "_fieldWithOuterSpaces_", "field_With0ut_Spaces"))
  }

}
