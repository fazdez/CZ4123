import java.io.*;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

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

    public ColumnStoreDiskEnhanced(HashMap<String, Integer> columnDataTypes) {
        super(columnDataTypes);
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
            toStore = timeStamp.toEpochSecond(ZoneOffset.of(ZoneId.systemDefault().getId()));
        }

        handleStoreLong(fileOutputStream, toStore);
    }

    @Override
    public void store(String column, String value) {
        try {
            File columnFile = new File(column+".store");
            columnFile.createNewFile();
            if (!columnFile.setWritable(true) || !columnFile.setReadable(true)) {
                System.out.println("Could not set read/write to file");
                return;
            }
            FileOutputStream outputStream = new FileOutputStream(columnFile, true);
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

    public List<Output> getExtremeValues(int year, String station) {
        List<Integer> qualifiedIndexes = getYear(year);
        qualifiedIndexes = getStation(station, qualifiedIndexes);
        for (int month = 1; month <= 12; month++) {
            List<Integer> monthIndexes = getMonth(month, qualifiedIndexes);
            HashMap<String, List<Integer>> scanResultsForTemp = sharedScanningMaxMin("Temperature", monthIndexes);
            HashMap<String, List<Integer>> scanResultsForHumidity = sharedScanningMaxMin("Humidity", monthIndexes);

        }
        return null;
    }

    private List<Integer> getYear(int year) {
        List<Integer> results = new ArrayList<>();
        try {
            FileInputStream inputStream = new FileInputStream("Timestamp.store");
            ByteBuffer bbf = ByteBuffer.allocate(BUFFER_SIZE);
            int index = 0;
            long startRange = LocalDateTime.of(year, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.of(ZoneId.systemDefault().getId()));
            long endRange = LocalDateTime.of(year, 12, 31, 23, 59, 59).toEpochSecond(ZoneOffset.of(ZoneId.systemDefault().getId()));
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
            RandomAccessFile fileInput = new RandomAccessFile("Station.store", "r");
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
            RandomAccessFile fileInput = new RandomAccessFile("Station.store", "r");
            ByteBuffer bbf = ByteBuffer.allocate(8);
            for(int index: indexesToCheck) {
                bbf.clear();
                //since station is just 1 byte, can access directly via index
                fileInput.seek(index*8L);
                fileInput.read(bbf.array());
                long value = bbf.getLong(0);
                if (value == 0) { continue; } //null value
                LocalDateTime timestamp = LocalDateTime.ofEpochSecond(value, 0, ZoneOffset.of(ZoneId.systemDefault().getId()));
                if (timestamp.getMonthValue() == month) { results.add(index); }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    private HashMap<String, List<Integer>> sharedScanningMaxMin(String column, List<Integer> indexesToCheck) {
        HashMap<String, List<Integer>> results = new HashMap<>();
        results.put("min", new ArrayList<>());
        results.put("max", new ArrayList<>());

        try {
            RandomAccessFile fileInput = new RandomAccessFile(column+".store", "r");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }
}
