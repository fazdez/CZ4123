import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Predicate;

public abstract class ColumnStoreAbstract {
    public static final int STRING_DATATYPE = 0;
    public static final int INTEGER_DATATYPE = 1;
    public static final int FLOAT_DATATYPE = 2;
    public static final int TIME_DATATYPE= 3;
    protected static final String DTFORMATSTRING = "yyyy-MM-dd HH:mm";

    protected final Set<String> columnHeaders;
    protected final HashMap<String, Integer> columnDataTypes;

    public ColumnStoreAbstract(@NotNull HashMap<String, Integer> columnDataTypes) {
        columnHeaders = columnDataTypes.keySet();
        this.columnDataTypes = columnDataTypes;
    }

    /**
     * Parses the CSV file and stores into the column store, using {@link #storeAll(HashMap)}
     * @param filepath
     * @throws FileNotFoundException
     */
    public void addCSVData(String filepath) throws FileNotFoundException {
        final String separator = ",";
        File file = new File(filepath);
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
        HashMap<String, List<String>> buffer = new HashMap<>();
        for(String column: columnHeaders) {
            buffer.put(column, new ArrayList<>());
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
                buffer.get(column).add(value);
                i++;
            }

            while (i < incomingColumnHeaders.size()) { //means this datum has less columns that what is provided, add null values
                buffer.get(incomingColumnHeaders.get(i)).add("M");
                i++;
            }
        }
        
        storeAll(buffer);
        fileReader.close();
    }

    /**
     * Given a value string and the corresponding column, store into data storage.
     * @param column the column that this value belongs to
     * @param value the value in string
     */
    protected abstract void store(String column, String value);

    /**
     * Given a map of columns to its values (in String), store all into the data storage.
     * @param buffer the map of columns to its values
     */
    protected abstract void storeAll(HashMap<String, List<String>> buffer);

    public abstract List<Integer> filter(String column, Predicate<Object> predicate);

    public abstract List<Integer> filter(String column, Predicate<Object> predicate, List<Integer> indexesToCheck);

    public abstract List<Integer> getMax(String column, List<Integer> indexesToCheck);

    public abstract List<Integer> getMin(String column, List<Integer> indexesToCheck);

    /**
     * gets the value from a column based on the index.
     * @param column column to retrieve
     * @param index index to retrieve
     * @return The object in that index
     */
    public abstract Object getValue(String column, int index);

    /**
     * Prints the head of the data (i.e. from index 0) until the specified index.
     * @param until the index to print until.
     */
    public abstract void printHead(int until);

    /**
     * Checks if the column was registered with this column store or not.
     * @param column the column to check
     * @return true if column is not registered (i.e. invalid).
     */
    protected boolean isInvalidColumn(String column) {
        return !columnHeaders.contains(column);
    }

    /**
     * Based on the value string and column type, cast this value string to the appropriate type.
     * Additionally, checks the validation of value string.
     * @param column The name of the column
     * @param value The value to cast
     * @return An object cast with the appropriate type. Null if Object could not be cast or value string == 'M' or empty string.
     */
    protected Object castValueAccordingToColumnType(String column, String value) {
        try {
            if (Objects.equals(value, "") || Objects.equals(value, "M")) {
                return null;
            }

            switch (columnDataTypes.get(column)) {
                case STRING_DATATYPE -> { return value; }
                case INTEGER_DATATYPE -> { return Integer.parseInt(value); }
                case FLOAT_DATATYPE -> { return Float.parseFloat(value); }
                case TIME_DATATYPE -> { return LocalDateTime.parse(value, DateTimeFormatter.ofPattern(DTFORMATSTRING)); }
                default -> throw new IllegalArgumentException(String.format("No such data type for column (%s) registered. Defaulting to string...", column));
            }
        } catch (NumberFormatException e) {
            System.out.printf("Column (%s) has value (%s) that is not a number.\n", column, value);
        } catch (DateTimeParseException e) {
            System.out.printf("Column (%s) has date time (%s) that is not equal to the format enforced (%s).\n", column, value, DTFORMATSTRING);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        } catch(Exception e) { //any other exception
            e.printStackTrace();
        }

        return null;
    }

    /**
     * @param column column to check
     * @return true if column data type is not a {@link #INTEGER_DATATYPE integer} or {@link #FLOAT_DATATYPE float}.
     */
    protected boolean isNotNumberDataType(String column) {
        return columnDataTypes.get(column) != INTEGER_DATATYPE && columnDataTypes.get(column) != FLOAT_DATATYPE;
    }

    /**
     * Useful in {@link #getMax(String, List)} and {@link #getMin(String, List)} functions.
     * So that code does not have to be repeated.
     * @param column column to check
     * @return true if column data type is a number.
     */
    protected boolean validationCheckForMinMax(String column) {
        if (isInvalidColumn(column)) {
            System.out.println("Column is not registered with this column store.");
            return false;
        }

        if (isNotNumberDataType(column)) {
            System.out.println("Cannot perform get max operation on a column whose data are not numbers.");
            return false;
        }

        return true;
    }
}
