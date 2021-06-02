package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.WeatherFactorsCounter.countWeatherFactors;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkTest {

    final String testString1 = "1,01.06.2021 10:30:00.000,area1,sensor10_pres,83\n";
    final String testString2 = "2,01.06.2021 10:40:00.000,area1,sensor9_pres,43\n";
    final String testString3 = "3,01.06.2021 11:30:00.000,area2,sensor9_temp,540\n";
    final String testString4 = "4,01.06.2021 12:30:00.000,area3,sensor8_hum,40\n";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testOneLog() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1));
        JavaRDD<Row> result = countWeatherFactors(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getString(0).equals("01.06.2021 10:00:00.000");
        assert rowList.iterator().next().getString(1).equals("area1");
        assert rowList.iterator().next().getString(2).equals("pres");
        assert rowList.iterator().next().getDouble(3) == 83.0;
    }

    @Test
    public void testTwoLogsSameTime(){

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString2));
        JavaRDD<Row> result = countWeatherFactors(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getString(0).equals("01.06.2021 10:00:00.000");
        assert rowList.iterator().next().getString(1).equals("area1");
        assert rowList.iterator().next().getString(2).equals("pres");
        assert rowList.iterator().next().getDouble(3) == 63.0;
    }

    @Test
    public void testTwoLogsDifferentTime(){

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString3));
        JavaRDD<Row> result = countWeatherFactors(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getString(0).equals("01.06.2021 10:00:00.000");
        assert firstRow.getString(1).equals("area1");
        assert firstRow.getString(2).equals("pres");
        assert firstRow.getDouble(3) == 83.0;

        assert secondRow.getString(0).equals("01.06.2021 11:00:00.000");
        assert secondRow.getString(1).equals("area2");
        assert secondRow.getString(2).equals("temp");
        assert secondRow.getDouble(3) == 540.0;
    }

    @Test
    public void testFourLogs(){

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString2, testString3, testString4));
        JavaRDD<Row> result = countWeatherFactors(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);
        Row thirdRow = rowList.get(2);

        assert firstRow.getString(0).equals("01.06.2021 10:00:00.000");
        assert firstRow.getString(1).equals("area1");
        assert firstRow.getString(2).equals("pres");
        assert firstRow.getDouble(3) == 63.0;

        assert secondRow.getString(0).equals("01.06.2021 11:00:00.000");
        assert secondRow.getString(1).equals("area2");
        assert secondRow.getString(2).equals("temp");
        assert secondRow.getDouble(3) == 540.0;

        assert thirdRow.getString(0).equals("01.06.2021 12:00:00.000");
        assert thirdRow.getString(1).equals("area3");
        assert thirdRow.getString(2).equals("hum");
        assert thirdRow.getDouble(3) == 40.0;
    }

}
