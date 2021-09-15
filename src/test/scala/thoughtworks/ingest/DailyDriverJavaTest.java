package thoughtworks.ingest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import thoughtworks.DefaultJavaTestWithSpark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DailyDriverJavaTest extends DefaultJavaTestWithSpark {
    @Test
    public void testDailyDriverDataIsStoredInParquetFormatWithBothRows() throws IOException {
//        Given("Input data in the expected format")

        Path rootDirectory = Files.createTempDirectory(this.getClass().getName());

        Path inputCsv = Files.createFile(rootDirectory.resolve("input.csv"));
        Path outputDirectory = rootDirectory.resolve("output");

        List<String> lines1 = Arrays.asList(
                "first_field,field with space, fieldWithOuterSpaces ",
                "3,1,4",
                "1,5,2");

        Files.write(inputCsv, lines1, StandardOpenOption.CREATE);

//        When("Daily Driver Ingestion is run")
        DailyDriverJava.run(spark, inputCsv.toUri().toString(), outputDirectory.toUri().toString());

//        Then("The data is stored in Parquet format with both rows")
        Dataset<Row> parquetDirectory = spark.read().parquet(outputDirectory.toUri().toString());
        assertEquals(parquetDirectory.count(), 2);

//        And("The column headers are renamed")
        assertArrayEquals(parquetDirectory.columns(), new String[]{"first_field", "field_with_space", "_fieldWithOuterSpaces_"});
    }
}
