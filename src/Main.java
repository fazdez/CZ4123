import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        HashMap<String, Integer> dataTypes = new HashMap<>();
        dataTypes.put("id", ColumnStoreAbstract.INTEGER_DATATYPE);
        dataTypes.put("Timestamp", ColumnStoreAbstract.TIME_DATATYPE);
        dataTypes.put("Station", ColumnStoreAbstract.STRING_DATATYPE);
        dataTypes.put("Temperature", ColumnStoreAbstract.FLOAT_DATATYPE);
        dataTypes.put("Humidity", ColumnStoreAbstract.FLOAT_DATATYPE);


        ColumnStoreAbstract csMM = new ColumnStoreMM(dataTypes);
        ColumnStoreAbstract csDisk = new ColumnStoreDisk(dataTypes);
        ColumnStoreAbstract csDiskEnhanced = new ColumnStoreDiskEnhanced(dataTypes);
        List<ColumnStoreAbstract> columnStores = Arrays.asList(csMM, csDisk, csDiskEnhanced);

        System.out.println("------Time Taken------");
        for (ColumnStoreAbstract cs: columnStores) {
            try {
                cs.addCSVData("SingaporeWeather.csv");
                LocalDateTime startTime = LocalDateTime.now();
                List<Output> results1 = getExtremeValues(cs, 2009, "Paya Lebar");
                List<Output> results2 = getExtremeValues(cs, 2019, "Paya Lebar");
                System.out.println(cs.getName() + ": " + startTime.until(LocalDateTime.now(), ChronoUnit.MILLIS) + "ms");

                writeOutput(cs.getName()+"/ScanResult.csv", results1);
                writeOutput(cs.getName()+"/ScanResult.csv", results2);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Gets the extreme values for each month in the year specified and station specified.
     * @param data the column store
     * @param year the year given
     * @param station the station given
     * @return a list of Output objects representing the extreme values.
     */
    private static List<Output> getExtremeValues(ColumnStoreAbstract data, int year, String station) {
        if (data instanceof ColumnStoreDiskEnhanced) {
            return ((ColumnStoreDiskEnhanced) data).getExtremeValues(year, station); //use custom implementation
        }


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

    /**
     * gets the extreme values for the specified month in the given column for the given station.
     * @param data the column store
     * @param currMonth the list of indexes representing the current month (and year)
     * @param column the column given
     * @param valueType "max" or "min"
     * @param stationName the station given
     * @return
     */
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

    /**
     * for each value in the output list, write to the file given.
     * @param filepath file to write
     * @param toWrite list of Output objects
     * @throws IOException
     */
    private static void writeOutput(String filepath, List<Output> toWrite) throws IOException {
        boolean initialized = true;
        File outputFile = new File(filepath);
        if (!outputFile.exists()) {
            initialized = false;
            outputFile.getParentFile().mkdirs();
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
