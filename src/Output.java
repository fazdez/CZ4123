import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This class represents a row in "ScanResult.csv"
 */
public class Output {
    public static final int MAX_HUMIDITY = 0;
    public static final int MAX_TEMP = 1;
    public static final int MIN_HUMIDITY = 2;
    public static final int MIN_TEMP = 3;

    LocalDateTime date;
    String stationName;
    int type;
    float value;

    public Output(LocalDateTime date, String station, int type, float value) {
        this.date = date;
        this.stationName = station;
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        String typeString = "";
        switch (type) {
            case MAX_HUMIDITY -> typeString = "Max Humidity";
            case MAX_TEMP -> typeString = "Max Temperature";
            case MIN_HUMIDITY -> typeString = "Min Humidity";
            case MIN_TEMP -> typeString = "Min Temperature";
            default -> typeString = "unknown";
        }

        return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) +
                "," + stationName +
                "," + typeString +
                "," + value;
    }
}
