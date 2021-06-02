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

@AllArgsConstructor
@Slf4j
public class LogLevelEventCounter {

    // Формат времени логов - н-р, 'Oct 26 13:54:06'
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("MMM dd HH:mm:ss")
            .parseDefaulting(YEAR, 2018)
            .toFormatter();

    /**
     * Функция подсчета количества логов разного уровня в час.
     * Парсит строку лога, в т.ч. уровень логирования и час, в который событие было зафиксировано.
     * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Row> countLogLevelPerHour(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());

        Dataset<LogLevelHour> logLevelHourDataset = words.map(s -> {
            String[] logFields = s.split(",");
            LocalDateTime date = LocalDateTime.parse(logFields[2], formatter);
            return new LogLevelHour(logFields[1], date.getHour());
            }, Encoders.bean(LogLevelHour.class))
                .coalesce(1);

        // Группирует по значениям часа и уровня логирования
        Dataset<Row> t = logLevelHourDataset.groupBy("hour", "logLevel")
                .count()
                .toDF("hour", "logLevel", "count")
                // сортируем по времени лога - для красоты
                .sort(functions.asc("hour"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
