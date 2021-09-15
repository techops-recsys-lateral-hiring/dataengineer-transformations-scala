package thoughtworks;

import org.apache.spark.sql.SparkSession;

public abstract class DefaultJavaTestWithSpark extends DefaultFeatureSpecWithSpark {

    protected transient SparkSession spark = spark();
}