package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CountWeatherFactor {
    String datetime;

    String area;

    String type;

    int sensorvalue;
}
