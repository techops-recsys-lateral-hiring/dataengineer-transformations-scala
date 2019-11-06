package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      dataSet
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      dataSet.as[String]
    }
  }
}
