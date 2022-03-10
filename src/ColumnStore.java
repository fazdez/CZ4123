import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Predicate;

public class ColumnStore {
    public static final int STRING_DATATYPE = 0;
    public static final int INTEGER_DATATYPE = 1;
    public static final int FLOAT_DATATYPE = 2;
    public static final int TIME_DATATYPE= 3;
    private static final String dtFormat = "yyyy-MM-dd HH:mm";


    private final boolean storeInMemory;
    private final Set<String> columnHeaders;
    private final HashMap<String, Integer> columnDataTypes;
    private final HashMap<String, List<Object>> buffer = new HashMap<>();

    /**
     * creates a new column store
     * @param isMemoryStore true if to store in memory, false if to store in disk
     * @param columnDataTypes a map of the known columns to its data type (in string)
     */
    public ColumnStore(boolean isMemoryStore, HashMap<String, Integer> columnDataTypes) {
        storeInMemory = isMemoryStore;
        columnHeaders = columnDataTypes.keySet();
        this.columnDataTypes = columnDataTypes;
        for (String columnHeader: columnHeaders) {
            buffer.put(columnHeader, new ArrayList<>());
        }
    }

    public List<Integer> filter(String column, Predicate<Object> predicate) {
        if (!storeInMemory) {
            readFromDisk(column); //store to buffer 'buffer'
        }
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < buffer.get(column).size(); i++) {
            Object value = buffer.get(column).get(i);
            if (value == null) { continue; }
            if (predicate.test(value)) {
                results.add(i);
            }
        }
        if (!storeInMemory) {
            buffer.clear(); //clear buffer 'buffer'
        }

        return results;
    }

    public List<Integer> filter(String column, Predicate<Object> predicate, List<Integer> indexesToCheck) {
        if (!storeInMemory) {
            readFromDisk(column); //store to buffer
        }

        List<Integer> results = new ArrayList<>();
        for (int index: indexesToCheck) {
            Object value = buffer.get(column).get(index);
            if (value == null) { continue; }
            if (predicate.test(value)) {
                results.add(index);
            }
        }

        if (!storeInMemory) {
            buffer.clear(); //clear buffer
        }

        return results;
    }

    public List<Integer> getMax(String column, List<Integer> indexesToCheck) {
        if (!storeInMemory) {
            readFromDisk(column); //store to buffer 'buffer'
        }
        if (columnDataTypes.get(column) != INTEGER_DATATYPE && columnDataTypes.get(column) != FLOAT_DATATYPE) {
            System.out.println("Cannot perform GET MAX on a column whose datatype is not a number!");
            return null;
        }

        List<Integer> result = new ArrayList<>();
        float maximum = Float.MIN_VALUE;
        for (int index: indexesToCheck) {
            Object value = buffer.get(column).get(index);
            float valueInFloat = (Float) value;
            if (valueInFloat == maximum) {
                result.add(index);
            } else if (valueInFloat > maximum) {
                maximum = valueInFloat;
                result.clear();
                result.add(index);
            }
        }

        if (!storeInMemory) {
            buffer.clear(); //clear buffer 'buffer'
        }

        return result;
    }

    public List<Integer> getMin(String column, List<Integer> indexesToCheck) {
        if (!storeInMemory) {
            readFromDisk(column); //store to buffer 'buffer'
        }
        if (columnDataTypes.get(column) != INTEGER_DATATYPE && columnDataTypes.get(column) != FLOAT_DATATYPE) {
            System.out.println("Cannot perform GET MAX on a column whose datatype is not a number!");
            return null;
        }

        List<Integer> result = new ArrayList<>();
        float minimum = Float.MAX_VALUE;
        for (int index: indexesToCheck) {
            Object value = buffer.get(column).get(index);
            float valueInFloat = (Float) value;
            if (valueInFloat == minimum) {
                result.add(index);
            } else if (valueInFloat < minimum) {
                minimum = valueInFloat;
                result.clear();
                result.add(index);
            }
        }

        if (!storeInMemory) {
            buffer.clear(); //clear buffer 'buffer'
        }
        return result;
    }

    public Object getValue(String column, int index) {
        if (!storeInMemory) {
            return getValueFromDisk(column, index);
        }
        return buffer.get(column).get(index);
    }

    /**
     * Prints the head of each column in the data, until the specified length.
     *
     * @param until the specified length
     */
    public void printHead(int until) {
        for (String column: columnHeaders) {
            if (!storeInMemory) {
                readFromDisk(column, until);
            }
            System.out.printf("%s: ", column);
            System.out.println(buffer.get(column).subList(0, until));
        }

        if (!storeInMemory) {
            buffer.clear();
        }
    }

    /**
     * For each datum in the csv file, add to the column store.
     * @param csvFile the relative filepath to the csv file
     */
    public void addCSVData(String csvFile) throws FileNotFoundException {
        final String separator = ",";
        File file = new File(csvFile);
        Scanner fileReader = new Scanner(file);

        if (!fileReader.hasNext()) {
            System.out.println("could not csv decode file: no column headers");
            return;
        }

        List<String> incomingColumnHeaders = List.of(fileReader.nextLine().split(separator)); //get the column headers
        if (columnHeaders != null && columnHeaders.equals(Set.of(incomingColumnHeaders))) {
            System.out.println("Incoming CSV data has different format from current csv data");
            return;
        }

        while (fileReader.hasNext()) {
            List<String> nextDatum = List.of(fileReader.nextLine().split(separator));
            int i = 0;
            while(i < nextDatum.size()) {
                if (i >= incomingColumnHeaders.size()) { //means this datum has more columns that what is provided, skip to next line
                    break;
                }

                String value = nextDatum.get(i);
                String column = incomingColumnHeaders.get(i);
                storeToMemory(column, value);
                i++;
            }

            while (i < incomingColumnHeaders.size()) { //means this datum has less columns that what is provided, add null values
                storeToMemory(incomingColumnHeaders.get(i), "");
                i++;
            }
        }
        fileReader.close();

        if (!storeInMemory) { //i.e. if to store in disk, flush the memory into disk
            storeToDisk();
        }
    }

    private void storeToMemory(String column, String value) {
        try {
            if (Objects.equals(value, "") || Objects.equals(value, "M")) {
                buffer.get(column).add(null);
                return;
            }

            switch (columnDataTypes.get(column)) {
                case STRING_DATATYPE:
                    buffer.get(column).add(value);
                    break;
                case INTEGER_DATATYPE:
                    int toAddInt = Integer.parseInt(value);
                    buffer.get(column).add(toAddInt);
                    break;
                case FLOAT_DATATYPE:
                    float toAddFloat = Float.parseFloat(value);
                    buffer.get(column).add(toAddFloat);
                    break;
                case TIME_DATATYPE:
                    LocalDateTime toAddDateTime = LocalDateTime.parse(value, DateTimeFormatter.ofPattern(dtFormat));
                    buffer.get(column).add(toAddDateTime);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("No such data type for column (%s) registered. Defaulting to string...", column));
            }
        } catch (NumberFormatException e) {
            System.out.printf("Column (%s) has value (%s) that is not a number.\n", column, value);
            buffer.get(column).add(null);
        } catch (DateTimeParseException e) {
            System.out.printf("Column (%s) has date time (%s) that is not equal to the format enforced (%s).\n", column, value, dtFormat);
            buffer.get(column).add(null);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            buffer.get(column).add(value);
        } catch(Exception e) {
            //any other exception
//            e.printStackTrace();
        }
    }

    private void storeToDisk() {
        for (String column: columnHeaders) {
            try {
                File columnFile = new File(column+".csv");
                columnFile.createNewFile();
                columnFile.setWritable(true);
                columnFile.setReadable(true);
                FileOutputStream outputStream = new FileOutputStream(columnFile, false);
                for (Object toWrite: buffer.get(column)) {
                    if (toWrite == null) {
                        outputStream.write('M');
                        outputStream.write('\n');
                        continue;
                    }

                    if (columnDataTypes.get(column) == TIME_DATATYPE) {
                        LocalDateTime toWriteDateTime = (LocalDateTime) toWrite;
                        outputStream.write(toWriteDateTime.format(DateTimeFormatter.ofPattern(dtFormat)).getBytes(StandardCharsets.UTF_8));
                    } else {
                        outputStream.write(toWrite.toString().getBytes(StandardCharsets.UTF_8));
                    }

                    outputStream.write('\n');
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        buffer.clear(); // now "buffer" acts as buffer. clear the buffer once written to disk
    }

    private void readFromDisk(String column) {
        try{
            File file = new File(column+".csv");
            Scanner fileReader = new Scanner(file);
            buffer.put(column, new ArrayList<>());
            while(fileReader.hasNext()) {
                String value = fileReader.nextLine();
                storeToMemory(column, value); //store to memory buffer
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void readFromDisk(String column, int until) {
        try{
            File file = new File(column+".csv");
            Scanner fileReader = new Scanner(file);
            buffer.put(column, new ArrayList<>());
            while(fileReader.hasNext() && until > 0) {
                String value = fileReader.nextLine();
                storeToMemory(column, value); //store to memory buffer
                until--;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private Object getValueFromDisk(String column, int index) {
        try {
            File file = new File(column+".csv");
            Scanner fileReader = new Scanner(file);
            while (index > 0) {
                fileReader.nextLine();
                index--;
            }
            String result = fileReader.nextLine();
            if (Objects.equals(result, "M")) {
                return null;
            }
            switch(columnDataTypes.get(column)) {
                case INTEGER_DATATYPE -> {
                    return Integer.parseInt(result);
                }

                case FLOAT_DATATYPE -> {
                    return Float.parseFloat(result);
                }

                case TIME_DATATYPE -> {
                    return LocalDateTime.parse(result, DateTimeFormatter.ofPattern(dtFormat));
                }

                default -> {
                    return result;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
