package thoughtworks.ingest;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DailyDriverJava {

    static Logger log = LogManager.getRootLogger();

    public static void main(String[] args) {
        log.setLevel(Level.INFO);
        SparkSession spark = SparkSession.builder().appName("Skinny Pipeline: Ingest").getOrCreate();
        log.info("Application Initialized: " + spark.sparkContext().appName());

        if (args.length < 2) {
            log.warn("Input source and output path are required");
            System.exit(1);
        }

        final String inputSource = args[0];
        final String outputPath = args[1];
        run(spark, inputSource, outputPath);

        log.info("Application Done: " + spark.sparkContext().appName());
        spark.stop();
    }

    public static void run(SparkSession spark, String inputSource, String outputPath) {
        Dataset<Row> inputDataFrame = spark.read()
                .format("org.apache.spark.csv")
                .option("header", true)
                .csv(inputSource);
        formatColumnHeaders(inputDataFrame)
                .write()
                .parquet(outputPath);
    }

    private static Dataset<Row> formatColumnHeaders(Dataset<Row> dataFrame) {
        Dataset<Row> retDf = dataFrame;
        String[] columns = dataFrame.columns();
        for (String column : columns) {
            retDf = retDf.withColumnRenamed(column, column.replaceAll("\\s", "_"));
        }
        retDf.printSchema();
        return retDf;
    }

}
