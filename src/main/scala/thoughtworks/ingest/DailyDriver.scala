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

  def formatColumnHeaders(dataFrame: DataFrame): DataFrame = {
    var retDf = dataFrame
    for (column <- retDf.columns) {
      retDf = retDf.withColumnRenamed(column, column.replaceAll("\\s", "_"))
    }
    retDf.printSchema()
    retDf
  }
}
