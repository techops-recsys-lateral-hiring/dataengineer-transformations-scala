package thoughtworks.citibike;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;
import thoughtworks.DefaultJavaTestWithSpark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.junit.Assert.*;

public class CitibikeTransformerJavaTest extends DefaultJavaTestWithSpark {

    List<Row> sampleCitibikeData = Arrays.asList(
            RowFactory.create(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2),
            RowFactory.create(1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1),
            RowFactory.create(1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2)
    );

    List<StructField> citibikeBaseDataColumns = Arrays.asList(
            DataTypes.createStructField("tripduration", DataTypes.IntegerType, false),
            DataTypes.createStructField("starttime", DataTypes.StringType, true),
            DataTypes.createStructField("stoptime", DataTypes.StringType, true),
            DataTypes.createStructField("start_station_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("start_station_name", DataTypes.StringType, true),
            DataTypes.createStructField("start_station_latitude", DoubleType, false),
            DataTypes.createStructField("start_station_longitude", DoubleType, false),
            DataTypes.createStructField("end_station_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("end_station_name", DataTypes.StringType, true),
            DataTypes.createStructField("end_station_latitude", DoubleType, false),
            DataTypes.createStructField("end_station_longitude", DoubleType, false),
            DataTypes.createStructField("bikeid", DataTypes.IntegerType, false),
            DataTypes.createStructField("usertype", DataTypes.StringType, true),
            DataTypes.createStructField("birth_year", DataTypes.IntegerType, false),
            DataTypes.createStructField("gender", DataTypes.IntegerType, false)
    );

    StructType citibikeBaseDataColumnsSchema = DataTypes.createStructType(citibikeBaseDataColumns);

    @Test
    public void testCitibikeTransformationShouldRetainOldDataWhileReadingNewData() throws IOException {

//        Given("Ingested data")
        Directories directories = RootDirectory.at("Citibike").createDirectories();
        String ingestDir = directories.ingest;
        String transformDir = directories.transform;

        Dataset<Row> inputDF = spark.createDataFrame(sampleCitibikeData, citibikeBaseDataColumnsSchema);
        inputDF.write().parquet(ingestDir);

//        When("Citibike Transformation is run for bikeshare data")
        CitibikeTransformerJava.run(spark, ingestDir, transformDir);

//        Then("The new data should have all of the old data")
        Dataset<Row> transformedDF = spark.read().parquet(transformDir);
        transformedDF.show();

        Column[] columnStream = citibikeBaseDataColumns.stream().map(cN -> col(cN.name())).collect(Collectors.toList()).stream().toArray(Column[]::new);
        Row[] newColumns = transformedDF.select(columnStream).collectAsList().stream().toArray(Row[]::new);
        Row[] expectedColumns = sampleCitibikeData.stream().toArray(Row[]::new);
        assertArrayEquals(expectedColumns, newColumns);

        StructField[] transformedDFFields = transformedDF.schema().fields();
        Arrays.sort(transformedDFFields, Comparator.comparing(StructField::name));

        StructField[] inputDFFields = transformedDF.schema().fields();
        Arrays.sort(inputDFFields, Comparator.comparing(StructField::name));

        assertArrayEquals(transformedDFFields, inputDFFields);

    }

    @Ignore
    public void testDailyDriverTransformationShouldContainDistanceColumnData() throws IOException {
        Directories directories = RootDirectory.at("Citibike").createDirectories();
        String ingestDir = directories.ingest;
        String transformDir = directories.transform;

//        Given("Ingested data")

        Dataset<Row> inputDF = spark.createDataFrame(sampleCitibikeData, citibikeBaseDataColumnsSchema);
        inputDF.write().parquet(ingestDir);


//        When("Daily Driver Transformation is run for Bikeshare data")

        CitibikeTransformerJava.run(spark, ingestDir, transformDir);

//        Then("The data should contain a distance column")
        Dataset<Row> transformedDF = spark.read().parquet(transformDir);

        List<Row> expectedData = Arrays.asList(
                RowFactory.create(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2, 1.07),
                RowFactory.create(1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1, 0.92),
                RowFactory.create(1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2.0, 1.99)
        );
        Optional<StructField> distance = Arrays.stream(transformedDF.schema().fields()).filter(x -> x.name().equals("distance")).findFirst();
        assertTrue(distance.isPresent());
        assertEquals(distance.get(), new StructField("distance", DoubleType, true, Metadata.empty()));
        Row[] newColumns = transformedDF.collectAsList().stream().toArray(Row[]::new);

        Row[] expectedColumns = expectedData.stream().toArray(Row[]::new);
        assertArrayEquals(expectedColumns, newColumns);

    }

    static class RootDirectory {
        private String folderNameSuffix;

        private RootDirectory(String folderNameSuffix) {
            this.folderNameSuffix = folderNameSuffix;
        }

        public static RootDirectory at(String folderNameSuffix) {
            return new RootDirectory(folderNameSuffix);
        }

        public Directories createDirectories() throws IOException {
            Path rootDirectory = Files.createTempDirectory(this.getClass().getName() + folderNameSuffix);
            return new Directories(rootDirectory.resolve("ingest").toUri().toString(), rootDirectory.resolve("transform").toUri().toString());
        }
    }

    static class Directories {

        public String ingest;
        public String transform;

        public Directories(String ingest, String transform) {
            this.ingest = ingest;
            this.transform = transform;
        }
    }
}
