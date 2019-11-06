package thoughtworks.citibike

import java.nio.file.Files

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField}
import thoughtworks.DefaultFeatureSpecWithSpark

class CitibikeTransformerTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  val citibikeBaseDataColumns = Seq(
    "tripduration", "starttime", "stoptime", "start_station_id", "start_station_name", "start_station_latitude", "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude", "end_station_longitude", "bikeid", "usertype", "birth_year", "gender"
  )
  val sampleCitibikeData = Seq(
    (328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2),
    (1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1),
    (1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2)
  )

  feature("Citibike Transformer Application") {
    scenario("Citibike Transformer Should Maintain All Of The Data It Read") {

      Given("Ingested data")

      val (ingestDir, transformDir) = makeInputAndOutputDirectories("Citibike")
      val inputDF = sampleCitibikeData.toDF(citibikeBaseDataColumns: _*)
      inputDF.write.parquet(ingestDir)

      When("Citibike Transformation is run for bikeshare data")

      CitibikeTransformer.run(spark, ingestDir, transformDir)

      Then("The new data should have all of the old data")

      val transformedDF = spark.read.parquet(transformDir)
      transformedDF.show()

      val newColumns = transformedDF.select(citibikeBaseDataColumns.map(cN => col(cN)): _*).collect
      val expectedColumns = sampleCitibikeData.map(t => Row(t.productIterator.toList: _*)).toArray

      newColumns should be(expectedColumns)

      val fieldToFieldIgnoringNullable: StructField => StructField = field => StructField(field.name, field.dataType)
      transformedDF.schema.fields.map(fieldToFieldIgnoringNullable)should contain.allElementsOf(inputDF.schema
        .fields.map(fieldToFieldIgnoringNullable))
    }

    ignore("Citibike Advanced Acceptance Test") {
      val rootDirectory = Files.createTempDirectory(this.getClass.getName + "Citibike")
      val ingestedDir = rootDirectory.resolve("ingest")
      val transformedDir = rootDirectory.resolve("transform")

      Given("Ingested data")

      val inputDf = sampleCitibikeData.toDF(citibikeBaseDataColumns: _*)

      inputDf.write.parquet(ingestedDir.toUri.toString)


      When("Daily Driver Transformation is run for Bikeshare data")

      CitibikeTransformer.run(spark, ingestedDir.toUri.toString, transformedDir.toUri.toString)


      Then("The data should contain a distance column")

      val transformedDF = spark.read
        .parquet(transformedDir.toUri.toString)

      val expectedData = Array(
        Row(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2, 1.07),
        Row(1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1, 0.92),
        Row(1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2.0, 1.99)
      )

      transformedDF.schema("distance") should be(StructField("distance", DoubleType, nullable = true))
      transformedDF.collect should be(expectedData)
    }
  }


  private def makeInputAndOutputDirectories(folderNameSuffix: String): (String, String) = {
    val rootDirectory =
      Files.createTempDirectory(this.getClass.getName + folderNameSuffix)
    val ingestDir = rootDirectory.resolve("ingest")
    val transformDir = rootDirectory.resolve("transform")
    (ingestDir.toUri.toString, transformDir.toUri.toString)
  }
}