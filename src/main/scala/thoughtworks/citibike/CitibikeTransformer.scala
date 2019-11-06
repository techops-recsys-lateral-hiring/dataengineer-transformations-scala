package thoughtworks.citibike

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object CitibikeTransformer {
  val log = LogManager.getRootLogger
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
