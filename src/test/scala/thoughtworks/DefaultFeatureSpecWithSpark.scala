package thoughtworks

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, GivenWhenThen, Matchers}

class DefaultFeatureSpecWithSpark extends FeatureSpec with GivenWhenThen with BeforeAndAfterAll with Matchers { self: FeatureSpec =>

  private var _spark: SparkSession = null

  protected implicit def spark: SparkSession = _spark

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  protected override def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession.builder
        .appName("Spark Test App")
        .config("spark.driver.host","127.0.0.1")
        .master("local")
        .getOrCreate()
    }

    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }
}
