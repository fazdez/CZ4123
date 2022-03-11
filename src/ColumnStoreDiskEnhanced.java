import java.io.*;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * This is a class that was created specific to the problem at hand.
 *
 * Unlike ColumnStoreDisk and ColumnStoreMM, the class is not for general-use. The class
 * is just to show how to
 * 1. Perform shared scanning when calculating the extreme values.
 * 2. Compression of "Station" column from variable width (string) to a fixed 1-byte character.
 * 3. Fixing the variable-length string "Timestamp" to long (unix timestamp).
 */
public class ColumnStoreDiskEnhanced extends ColumnStoreDisk{
    private static final byte NULL_STATION = 'M';
    private static final byte PAYA_LEBAR_STATION = 'P';
    private static final byte CHANGI_STATION = 'C';
    private static final long NULL_TIMESTAMP = 0;
    private static final String MIN_KEY = "min";
    private static final String MAX_KEY = "max";
    private static final ZoneOffset z = ZoneOffset.ofHours(+8);

    public ColumnStoreDiskEnhanced(HashMap<String, Integer> columnDataTypes) {
        super(columnDataTypes);
    }

    @Override
    public void store(FileOutputStream outputStream, String column, String value) {
        try {
            Object toStore = castValueAccordingToColumnType(column, value);
            switch(column) {
                case "Timestamp" -> {
                    if (toStore == null) { handleStoreTimestamp(outputStream, null); }
                    else { handleStoreTimestamp(outputStream, (LocalDateTime)toStore);}
                }

                case "Station" -> {
                    if (toStore == null) { handleStoreStation(outputStream, "M"); }
                    else { handleStoreStation(outputStream, (String) toStore);}
                }

                case "id" -> {
                    if (toStore == null) { handleStoreInteger(outputStream, Integer.MIN_VALUE); }
                    else { handleStoreInteger(outputStream, (int) toStore); }
                }

                case "Temperature", "Humidity" -> {
                    if (toStore == null) { handleStoreFloat(outputStream, Float.NaN); }
                    else { handleStoreFloat(outputStream, (float)toStore); }
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getName() {
        return "enhanced_disk";
    }

    private void handleStoreStation(FileOutputStream fileOutputStream, String stationName) {
        if (stationName.isBlank()) {
            stationName = "M"; //represents null
        }
        byte toStore = stationName.getBytes()[0]; //then 'P' represents paya lebar, 'C' represents changi, 'M' is null
        try {
            fileOutputStream.write(toStore);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleStoreTimestamp(FileOutputStream fileOutputStream, LocalDateTime timeStamp) {
        long toStore = 0;
        if (timeStamp != null) {
            toStore = timeStamp.toEpochSecond(z);
        }

        handleStoreLong(fileOutputStream, toStore);
    }

    /**
     * Gets the extreme values of Max temp, min temp, max humidity, min humidity for each month, in the year and station specified.
     *
     * For each month, a thread is run to find these values.
     * @param year the year to check
     * @param station the station to check
     * @return the results
     */
    public List<Output> getExtremeValues(int year, String station) {
        List<Integer> qualifiedIndexes = getYear(year);
        qualifiedIndexes = getStation(station, qualifiedIndexes);
        List<Output> results = new ArrayList<>();

        List<Thread> threads = new ArrayList<>();
        for (int month = 1; month <= 12; month++) {
            int finalMonth = month; // can only pass 'final' variables into lambda function
            List<Integer> finalQualifiedIndexes = new ArrayList<>(qualifiedIndexes);
            threads.add(new Thread(() -> scanValues(finalMonth, finalQualifiedIndexes, results, station)));
        }

        for (Thread t: threads) {
            t.start();
        }

        for (Thread t: threads) {
            try {
                t.join();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        return results;
    }

    private List<Integer> getYear(int year) {
        List<Integer> results = new ArrayList<>();
        try {
            FileInputStream inputStream = new FileInputStream(getName()+"/Timestamp.store");
            ByteBuffer bbf = ByteBuffer.allocate(BUFFER_SIZE);
            int index = 0;
            long startRange = LocalDateTime.of(year, 1, 1, 0, 0, 0).toEpochSecond(z);
            long endRange = LocalDateTime.of(year, 12, 31, 23, 59, 59).toEpochSecond(z);
            while (inputStream.read(bbf.array()) != -1) {
                bbf.position(0);
                while (bbf.hasRemaining()) {
                    long value = bbf.getLong();
                    if (value >= startRange && value <= endRange) {
                        results.add(index);
                    }
                    index++;
                }
                bbf.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    private List<Integer> getStation(String station, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        try {
            RandomAccessFile fileInput = new RandomAccessFile(getName()+"/Station.store", "r");
            for(int index: indexesToCheck) {
                //since station is just 1 byte, can access directly via index
                fileInput.seek(index);
                byte value = fileInput.readByte();
                if (Objects.equals(station, "Paya Lebar") && value == PAYA_LEBAR_STATION) {
                    results.add(index);
                } else if (Objects.equals(station, "Changi") && value == CHANGI_STATION) {
                    results.add(index);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    private List<Integer> getMonth(int month, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        try {
            RandomAccessFile fileInput = new RandomAccessFile(getName()+"/Timestamp.store", "r");
            ByteBuffer bbf = ByteBuffer.allocate(8);
            for(int index: indexesToCheck) {
                bbf.clear();
                //since station is just 1 byte, can access directly via index
                fileInput.seek(index*8L);
                fileInput.read(bbf.array());
                long value = bbf.getLong(0);
                if (value == 0) { continue; } //null value
                LocalDateTime timestamp = LocalDateTime.ofEpochSecond(value, 0, z);
                if (timestamp.getMonthValue() == month) { results.add(index); }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    private HashMap<String, List<Integer>> sharedScanningMaxMin(String column, List<Integer> indexesToCheck) {
        HashMap<String, List<Integer>> results = new HashMap<>();
        results.put(MIN_KEY, new ArrayList<>());
        results.put(MAX_KEY, new ArrayList<>());

        try {
            RandomAccessFile fileInput = new RandomAccessFile(getName()+"/"+column+".store", "r");
            ByteBuffer bbf = ByteBuffer.allocate(4);
            float min = Float.MAX_VALUE;
            float max = Float.MIN_VALUE;

            for (int index: indexesToCheck) { //shared scanning
                fileInput.seek(index*4L);
                fileInput.read(bbf.array());
                float value = bbf.getFloat(0);
                if(Float.isNaN(value)) { continue; } // null value

                if (value == min) { results.get(MIN_KEY).add(index); }
                else if (value < min) {
                    results.get(MIN_KEY).clear();
                    results.get(MIN_KEY).add(index);
                    min = value;
                }

                if (value == max) { results.get(MAX_KEY).add(index); }
                else if ( value > max) {
                    results.get(MAX_KEY).clear();
                    results.get(MAX_KEY).add(index);
                    max =  value;
                }
                bbf.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    private void scanValues(int month, List<Integer> qualifiedIndexes, List<Output> results, String station) {
        List<Integer> monthIndexes = getMonth(month, qualifiedIndexes);
        HashMap<String, List<Integer>> scanResultsForTemp = sharedScanningMaxMin("Temperature", monthIndexes);
        HashMap<String, List<Integer>> scanResultsForHumidity = sharedScanningMaxMin("Humidity", monthIndexes);

        try {
            RandomAccessFile tempFile = new RandomAccessFile(getName()+"/Temperature.store", "r");
            RandomAccessFile humidityFile = new RandomAccessFile(getName()+"/Humidity.store", "r");
            RandomAccessFile timeFile = new RandomAccessFile(getName()+"/Timestamp.store", "r");

            addResults(results, scanResultsForHumidity.get(MAX_KEY), humidityFile, timeFile, station, Output.MAX_HUMIDITY);
            addResults(results, scanResultsForHumidity.get(MIN_KEY), humidityFile, timeFile, station, Output.MIN_HUMIDITY);
            addResults(results, scanResultsForTemp.get(MAX_KEY), tempFile, timeFile, station, Output.MAX_TEMP);
            addResults(results, scanResultsForTemp.get(MIN_KEY), tempFile, timeFile, station, Output.MIN_TEMP);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addResults(List<Output> results, List<Integer> indexes, RandomAccessFile fileInput, RandomAccessFile timeFile, String station, int type) {
        try {
            List<Output> toAdd = new ArrayList<>();
            ByteBuffer bbfValue = ByteBuffer.allocate(4);
            ByteBuffer bbfTimestamp = ByteBuffer.allocate(8);

            // because we might get duplicate days, as each day has 48 different times.
            // need to filter out duplicate days
            HashSet<Integer> daysAdded = new HashSet<>();

            for (int index: indexes) {
                fileInput.seek(index*4L);
                timeFile.seek(index*8L);

                fileInput.read(bbfValue.array());
                timeFile.read(bbfTimestamp.array());

                float value = bbfValue.getFloat(0);
                long unixTimestamp = bbfTimestamp.getLong(0);
                LocalDateTime timestamp = LocalDateTime.ofEpochSecond(unixTimestamp, 0, z);
                if (!daysAdded.contains(timestamp.getDayOfMonth())) {
                    toAdd.add(new Output(timestamp, station, type, value));
                    daysAdded.add(timestamp.getDayOfMonth());
                }
                bbfTimestamp.clear();
                bbfValue.clear();
            }


            addToListSync(results, toAdd);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * concatenate a list with the list to modify in a synchronized manner.
     * @param list list to modify
     * @param toAdd the list to add
     */
    private synchronized void addToListSync(List<Output> list, List<Output> toAdd) {
        list.addAll(toAdd);
    }
}
