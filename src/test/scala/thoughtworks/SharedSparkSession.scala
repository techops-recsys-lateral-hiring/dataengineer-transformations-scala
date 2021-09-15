package thoughtworks
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, Suite}

trait SharedSparkSession
  extends FeatureSpec with BeforeAndAfterAll {

  private var _spark: SparkSession = null

  protected implicit def spark: SparkSession = _spark

  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected override def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession.builder.master("local[2]").appName(getClass.getSimpleName).getOrCreate
    }

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
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