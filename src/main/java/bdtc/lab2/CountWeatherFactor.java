package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LogLevelHour {

    // Уровень логирования
    private String logLevel;

    // Час, в который произошло событие
    private int hour;
}
