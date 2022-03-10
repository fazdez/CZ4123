import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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

        ByteBuffer bbf = ByteBuffer.allocate(8);
        bbf.putLong(toStore);

        try {
            fileOutputStream.write(bbf.array());
        } catch (Exception e) {
            e.printStackTrace();
        }
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

                case "Station" -> {}
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
