import java.util.*;
import java.util.function.Predicate;

/**
 * A column store implementation where the data is stored in main memory.
 */
public class ColumnStoreMM extends ColumnStoreAbstract {
    private final HashMap<String, List<Object>> data = new HashMap<>();

    public ColumnStoreMM(HashMap<String, Integer> columnDataTypes) {
        super(columnDataTypes);
        for (String columnHeader: columnHeaders) {
            data.put(columnHeader, new ArrayList<>()); // initialize the data array with empty lists.
        }
    }

    @Override
    protected void store(String column, String value) {
        if (isInvalidColumn(column)) {
            System.out.println("Column is not registered with this column store.");
        } else {
            Object toAdd = castValueAccordingToColumnType(column, value);
            data.get(column).add(toAdd);
        }
    }

    @Override
    protected void storeAll(HashMap<String, List<String>> buffer) {
        for(String column: buffer.keySet()) {
            for(String value: buffer.get(column)) {
                store(column, value);
            }
        }
    }

    @Override
    public List<Integer> filter(String column, Predicate<Object> predicate) {
        List<Integer> results = new ArrayList<>();
        if (isInvalidColumn(column)) {
            System.out.println("Column is not registered with this column store.");
            return results;
        }

        for (int i = 0; i < data.get(column).size(); i++) {
            Object value = data.get(column).get(i);
            if (value == null) { continue; }
            if (predicate.test(value)) {
                results.add(i);
            }
        }
        return results;
    }

    @Override
    public List<Integer> filter(String column, Predicate<Object> predicate, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        if (isInvalidColumn(column)) {
            System.out.println("Column is not registered with this column store.");
            return results;
        }

        for (int index: indexesToCheck) {
            Object value = data.get(column).get(index);
            if (value == null) { continue; }
            if (predicate.test(value)) {
                results.add(index);
            }
        }
        return results;
    }

    @Override
    public List<Integer> getMax(String column, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        if (!validationCheckForMinMax(column)) { return results; } //return empty list if validation check fails

        float maximum = Float.MIN_VALUE;
        for (int index: indexesToCheck) {
            if (data.get(column).get(index) == null) { continue; }
            float value = (float) data.get(column).get(index);
            if (value == maximum) {
                results.add(index);
            } else if (value > maximum) {
                maximum = value;
                results.clear();
                results.add(index);
            }
        }

        return results;
    }

    @Override
    public List<Integer> getMin(String column, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        if (!validationCheckForMinMax(column)) { return results; } //return empty list if validation check fails

        float minimum = Float.MAX_VALUE;
        for (int index: indexesToCheck) {
            if (data.get(column).get(index) == null) { continue; }
            float value = (float) data.get(column).get(index);
            if (value == minimum) {
                results.add(index);
            } else if (value < minimum) {
                minimum = value;
                results.clear();
                results.add(index);
            }
        }

        return results;
    }

    @Override
    public String getName() {
        return "main_memory";
    }

    @Override
    public Object getValue(String column, int index) {
        return data.get(column).get(index);
    }

    @Override
    public void printHead(int until) {
        for (String column: columnHeaders) {
            System.out.printf("%s: ", column);
            System.out.println(data.get(column).subList(0, until));
        }
    }
}
