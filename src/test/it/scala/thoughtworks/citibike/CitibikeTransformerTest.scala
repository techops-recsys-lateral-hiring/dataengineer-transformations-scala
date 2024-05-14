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

package it.scala.thoughtworks.citibike

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import thoughtworks.DefaultFeatureSpecWithSpark
import thoughtworks.citibike.CitibikeTransformer

import java.nio.file.Files

class CitibikeTransformerTest extends AnyFunSuite with DefaultFeatureSpecWithSpark {
  val citibikeBaseSchema = List(
    StructField("tripduration", IntegerType, nullable = true),
    StructField("starttime", StringType, nullable = true),
    StructField("stoptime", StringType, nullable = true),
    StructField("start_station_id", IntegerType, nullable = true),
    StructField("start_station_name", StringType, nullable = true),
    StructField("start_station_latitude", DoubleType, nullable = true),
    StructField("start_station_longitude", DoubleType, nullable = true),
    StructField("end_station_id", IntegerType, nullable = true),
    StructField("end_station_name", StringType, nullable = true),
    StructField("end_station_latitude", DoubleType, nullable = true),
    StructField("end_station_longitude", DoubleType, nullable = true),
    StructField("bikeid", IntegerType, nullable = true),
    StructField("usertype", StringType, nullable = true),
    StructField("birth_year", IntegerType, nullable = true),
    StructField("gender", IntegerType, nullable = true),
  )

  val sampleCitibikeData = Seq(
    Row(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968
      , -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2),
    Row(1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799
      , 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1),
    Row(1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487
      , "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2)
  )

  test("Citibike Transformer Should Maintain All Of The Data It Read") {

    val (ingestDir, transformDir) = makeInputAndOutputDirectories()
    val inputDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(sampleCitibikeData),
      StructType(citibikeBaseSchema))
    inputDF.write.parquet(ingestDir)

    CitibikeTransformer.run(spark, ingestDir, transformDir)
    val transformedDF = spark.read.parquet(transformDir)
    transformedDF.show()

    val anyMissingOriginalDataCount = inputDF.except(transformedDF).count()
    anyMissingOriginalDataCount should be(0)
    val fieldToFieldIgnoringNullable: StructField => StructField = field => StructField(field.name, field.dataType)
    transformedDF.schema.fields.map(fieldToFieldIgnoringNullable) should contain.allElementsOf(inputDF.schema
      .fields.map(fieldToFieldIgnoringNullable))

  }


  ignore("the distance column should be computed correctly") {
    val rootDirectory = Files.createTempDirectory(this.getClass.getName + "Citibike")
    val ingestedDir = rootDirectory.resolve("ingest")
    val transformedDir = rootDirectory.resolve("transform")

    val (ingestDir, transformDir) = makeInputAndOutputDirectories()
    val inputDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(sampleCitibikeData),
      StructType(citibikeBaseSchema))
    inputDF.write.parquet(ingestDir)

    CitibikeTransformer.run(spark, ingestDir, transformDir)

    val transformedDF = spark.read
      .parquet(transformDir)
    val expectedData = Array(
      Row(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968
        , -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984
        , 2, 1.07),
      Row(1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726
        , -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer"
        , 1971, 1, 0.92),
      Row(1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487
        , "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2.0, 1.99)
    )

    transformedDF.schema("distance") should be(StructField("distance", DoubleType, nullable = true))
    transformedDF.collect should be(expectedData)
  }

  private def makeInputAndOutputDirectories(): (String, String) = {
    val folderNameSuffix = "citibike"
    val rootDirectory =
      Files.createTempDirectory(this.getClass.getName + folderNameSuffix)
    val ingestDir = rootDirectory.resolve("ingest")
    val transformDir = rootDirectory.resolve("transform")
    (ingestDir.toUri.toString, transformDir.toUri.toString)
  }

}
