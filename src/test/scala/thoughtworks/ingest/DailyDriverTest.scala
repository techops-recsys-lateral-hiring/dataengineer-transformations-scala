package thoughtworks.ingest

import java.nio.file.{Files, StandardOpenOption}

import thoughtworks.DefaultFeatureSpecWithSpark

class DailyDriverTest extends DefaultFeatureSpecWithSpark {

  feature("Daily Driver Ingestion") {
    scenario("Normal use") {
      Given("Input data in the expected format")

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)

      val inputCsv = Files.createFile(rootDirectory.resolve("input.csv"))
      val outputDirectory = rootDirectory.resolve("output")
      import scala.collection.JavaConverters._
      val lines = List(
        "first_field,field with space, fieldWithOuterSpaces ",
        "3,1,4",
        "1,5,2"
      )
      Files.write(inputCsv, lines.asJava, StandardOpenOption.CREATE)

      When("Daily Driver Ingestion is run")
      DailyDriver.run(spark, inputCsv.toUri.toString, outputDirectory.toUri.toString)

      Then("The data is stored in Parquet format with both rows")
      val parquetDirectory = spark.read.parquet(outputDirectory.toUri.toString)
      parquetDirectory.count should be (2)

      And("The column headers are renamed")
      parquetDirectory.columns should be (Array("first_field", "field_with_space", "_fieldWithOuterSpaces_"))
    }
  }
}
