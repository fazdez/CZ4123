import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        HashMap<String, Integer> dataTypes = new HashMap<>();
        dataTypes.put("id", ColumnStore.INTEGER_DATATYPE);
        dataTypes.put("Timestamp", ColumnStore.TIME_DATATYPE);
        dataTypes.put("Station", ColumnStore.STRING_DATATYPE);
        dataTypes.put("Temperature", ColumnStore.FLOAT_DATATYPE);
        dataTypes.put("Humidity", ColumnStore.FLOAT_DATATYPE);

        ColumnStoreAbstract cs = new ColumnStoreDisk(dataTypes);
        try {
            cs.addCSVData("SingaporeWeather.csv");
        } catch(Exception e) {
            e.printStackTrace();
        }

//        System.out.println(cs.filter("Station", stn -> stn.equals("Paya Lebar")));
//        System.out.println(cs.filter("Timestamp", time -> ((LocalDateTime)time).getYear() == 2002 && ((LocalDateTime)time).getMonthValue() == 1 &&
//                ((LocalDateTime)time).getHour() == 0 && ((LocalDateTime)time).getMinute() == 0));
//        System.out.println(cs.filter("Humidity", value -> (float)value == 24.19F));
        cs.printHead(10);
        try {
            List<Output> results = getExtremeValues(cs, 2009, "Paya Lebar");
            writeOutput("ScanResult.csv", results);
            results = getExtremeValues(cs, 2019, "Paya Lebar");
            writeOutput("ScanResult.csv", results);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Output> getExtremeValues(ColumnStoreAbstract data, int year, String station) {
        List<Integer> yearIndices = data.filter("Timestamp", datum -> ((LocalDateTime)datum).getYear() == year);
        List<Integer> stationAndYearIndices = data.filter("Station", datum -> datum.equals(station), yearIndices);
        List<Output> result = new ArrayList<>();
        for (int month = 1; month <= 12; month++) {
            int m = month; //need to use a final variable in lambda functions
            List<Integer> currentMonthIndices = data.filter("Timestamp", datum -> ((LocalDateTime)datum).getMonth() == Month.of(m), stationAndYearIndices);
            result.addAll(processMonth(data, currentMonthIndices, "Humidity", "max", station));
            result.addAll(processMonth(data, currentMonthIndices, "Humidity", "min", station));
            result.addAll(processMonth(data, currentMonthIndices, "Temperature", "max", station));
            result.addAll(processMonth(data, currentMonthIndices, "Temperature", "min", station));
        }

        return result;
    }

    private static List<Output> processMonth(ColumnStoreAbstract data, List<Integer> currMonth, String column, String valueType, String stationName) {
        List<Output> result = new ArrayList<>();
        Set<Integer> addedDays = new HashSet<>();
        if (!Objects.equals(column, "Humidity") && !Objects.equals(column, "Temperature")) {
            System.out.println("This is a specific function that only accepts Humidity or Temperature columns.");
            return result;
        }

        if (!Objects.equals(valueType, "min") && !Objects.equals(valueType, "max")) {
            System.out.println("Value type must be 'min' or 'max' only!");
            return result;
        }

        List<Integer> qualifiedIndexes = Objects.equals(valueType, "max") ? data.getMax(column, currMonth) : data.getMin(column, currMonth);
        for (int index: qualifiedIndexes) {
            LocalDateTime timestamp = (LocalDateTime) data.getValue("Timestamp", index);
            if (addedDays.contains(timestamp.getDayOfMonth())) {
                continue; // we do not want duplicate days, we only want duplicate months
            } else { addedDays.add(timestamp.getDayOfMonth()); }

            float humidityValue = (float) data.getValue(column, index);
            int outputType = -1;
            if (Objects.equals(column, "Humidity")) {
                if (Objects.equals(valueType, "min")) { outputType = Output.MIN_HUMIDITY; }
                else { outputType = Output.MAX_HUMIDITY; }
            } else {
                if (Objects.equals(valueType, "min")) { outputType = Output.MIN_TEMP; }
                else { outputType = Output.MAX_TEMP; }
            }

            result.add(new Output(timestamp, stationName, outputType, humidityValue));
        }

        return result;
    }

    private static void writeOutput(String filepath, List<Output> toWrite) throws IOException {
        boolean initialized = true;
        File outputFile = new File(filepath);
        if (!outputFile.exists()) {
            initialized = false;
            outputFile.createNewFile();
        }

        if(!outputFile.setReadable(true) || !outputFile.setWritable(true)) {
            System.out.println("Could not set permissions for this file.");
            return;
        }

        FileOutputStream outputStream = new FileOutputStream(outputFile, true);
        if (!initialized) {
            outputStream.write("Date,Station,Category,Value\n".getBytes(StandardCharsets.UTF_8));
        }

        for (Output data: toWrite) {
            outputStream.write(data.toString().getBytes(StandardCharsets.UTF_8));
            outputStream.write('\n');
        }
    }
}
