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
