package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;

import static java.time.temporal.ChronoField.YEAR;
import static org.apache.spark.sql.functions.avg;

@AllArgsConstructor
@Slf4j
public class WeatherFactorsCounter {

    // Формат времени логов
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("dd.MM.yyyy HH:mm:ss.SSS")
            .toFormatter();

    /**
     * Функция обработки данных о погоде в регионе методом почасового усреднения текущих характеристик температуры, влажности, давления, получаемых с датчиков
     * Парсит строку лога, в т.ч. погодные характеристики и время, в которое они были зафиксированы.
     * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Row> countWeatherFactors(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());

        Dataset<CountWeatherFactor> WeatherFactorDataset = words.map(s -> {
            s = s.replace("[","");
            s = s.replace("]","");
            String[] logFields = s.split(",");
            // Преобразуем время с точностью до часа
            LocalDateTime date = LocalDateTime.parse(logFields[1], formatter).withMinute(0).withSecond(0).withNano(0);
            // Преобразуем тип сенсора
            if (logFields[3].contains("temp"))
                logFields[3] = "temp";
            if (logFields[3].contains("pres"))
                logFields[3] = "pres";
            if (logFields[3].contains("hum"))
                logFields[3] = "hum";
            return new CountWeatherFactor(date.format(formatter), logFields[2], logFields[3], Integer.parseInt(logFields[4]));
            }, Encoders.bean(CountWeatherFactor.class))
                .coalesce(1);

        // Группирует по значениям времени, области и типа метрики
        Dataset<Row> t = WeatherFactorDataset.groupBy("datetime", "area", "type")
                .agg(avg("sensorvalue").alias("sensorvalue"))
                .toDF("datetime","area","type","sensorvalue")
                // сортируем
                .sort(functions.asc("datetime"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
