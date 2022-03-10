import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Predicate;

public class ColumnStoreDisk extends ColumnStoreAbstract{
    protected static final int BUFFER_SIZE = 10240;

    public ColumnStoreDisk(HashMap<String, Integer> columnDataTypes) {
        super(columnDataTypes);
    }

    /**
     * Write an appropriate value to the outputStream given the column string and value string.
     * @param outputStream output to write to
     * @param column column name
     * @param value value string
     * @throws IOException
     */
    private void store(FileOutputStream outputStream, String column, String value) throws IOException {
        Object toAdd = castValueAccordingToColumnType(column, value);
        switch(columnDataTypes.get(column)) {
            case STRING_DATATYPE -> {
                //get its string value, each datum separated by new line.
                if (toAdd == null) {
                    outputStream.write("M".getBytes(StandardCharsets.UTF_8));
                } else {
                    outputStream.write(toAdd.toString().getBytes(StandardCharsets.UTF_8));
                }
                outputStream.write('\n');
            }

            case TIME_DATATYPE -> {
                if (toAdd == null) {
                    outputStream.write("M".getBytes(StandardCharsets.UTF_8));
                } else {
                    outputStream.write(((LocalDateTime)toAdd).format(DateTimeFormatter.ofPattern(DTFORMATSTRING)).getBytes(StandardCharsets.UTF_8));
                }
                outputStream.write('\n');
            }

            case INTEGER_DATATYPE -> {
                if (toAdd == null) { handleStoreInteger(outputStream, Integer.MIN_VALUE); }
                else { handleStoreInteger(outputStream, (int)toAdd); }
            }

            case FLOAT_DATATYPE -> {
                if (toAdd == null) { handleStoreFloat(outputStream, Float.NaN); }
                else { handleStoreFloat(outputStream, (float)toAdd); }
            }
            default -> outputStream.write("M\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    protected void store(String column, String value) {
        try {
            File columnFile = new File(column+".store");
            columnFile.createNewFile();
            if (!columnFile.setWritable(true) || !columnFile.setReadable(true)) {
                System.out.println("Could not set read/write to file");
                return;
            }
            FileOutputStream outputStream = new FileOutputStream(columnFile, true);
            store(outputStream, column, value);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void storeAll(HashMap<String, List<String>> buffer) {
        try {
            for(String column: buffer.keySet()) {
                File columnFile = new File(column+".store");
                columnFile.createNewFile();
                if (!columnFile.setWritable(true) || !columnFile.setReadable(true)) {
                    System.out.println("Could not set read/write to file");
                    return;
                }
                FileOutputStream outputStream = new FileOutputStream(columnFile, true);
                for (String value: buffer.get(column)) {
                    store(outputStream, column, value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public List<Integer> filter(String column, Predicate<Object> predicate) {
        try {
            File file = new File(column+".store");
            int idx = 0;
            List<Integer> result = new ArrayList<>();
            if (isNotNumberDataType(column)) { //use BufferedReader since it's string
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file), BUFFER_SIZE);
                while(true) {
                    String value = bufferedReader.readLine();
                    if (value == null) {
                        break;
                    }
                    Object toCheck;
                    if (!Objects.equals(value, "M")) {
                        if (columnDataTypes.get(column) == TIME_DATATYPE) {
                            toCheck = LocalDateTime.parse(value, DateTimeFormatter.ofPattern(DTFORMATSTRING));
                        } else {
                            toCheck = value;
                        }
                        if (predicate.test(toCheck)) { result.add(idx); }
                    }
                    idx++;
                }
            } else { //use ByteBuffer and FileInputStream
                FileInputStream inputStream = new FileInputStream(file);
                ByteBuffer bbf = ByteBuffer.allocate(BUFFER_SIZE);

                while (inputStream.read(bbf.array()) != -1) { //there's still stuff to read
                    bbf.position(0); // reset the position to 0
                    while(bbf.hasRemaining()) {
                        byte[] tempBuffer = new byte[4];
                        bbf.get(tempBuffer);
                        Object toCheck = convertBytesToNumber(tempBuffer, columnDataTypes.get(column));
                        if (toCheck != null && predicate.test(toCheck)) {
                            result.add(idx);
                        }
                        idx++;
                    }
                    bbf.clear(); //clear the data in the bytebuffer to prepare for next read
                }
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Integer> filter(String column, Predicate<Object> predicate, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        try {
            if (isNotNumberDataType(column)) {
                File file = new File(column+".store");
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file), BUFFER_SIZE);
                int currIndex = 0;
                for (int indexToCheck: indexesToCheck) {
                    Object toCheck;
                    while (currIndex != indexToCheck) {
                        if(bufferedReader.readLine() == null) {
                            //end of file reached.
                            System.out.println("Index to check is out of bounds!");
                            return results;
                        }
                        currIndex++;
                    }

                    String value = bufferedReader.readLine();
                    currIndex++;
                    if (Objects.equals(value, "M")) { continue; } //null value, predicate will always be false. can skip to next index to check
                    if (columnDataTypes.get(column) == STRING_DATATYPE) { toCheck = value; }
                    else { toCheck = LocalDateTime.parse(value, DateTimeFormatter.ofPattern(DTFORMATSTRING)); }

                    if (predicate.test(toCheck)) { results.add(indexToCheck); }
                }
            } else { //values are stored directly, each taking up 4 bytes. Can skip to index using fileInputStream
                RandomAccessFile fileInputStream = new RandomAccessFile(column+".store", "r");
                ByteBuffer bbf = ByteBuffer.allocate(4);
                for (int indexToCheck: indexesToCheck) {
                    //can access directly
                    fileInputStream.seek(indexToCheck*4L);
                    if(fileInputStream.read(bbf.array()) != 4) {
                        System.out.println("Did not read 4 bytes when getting a number value from file.");
                    }
                    Object toCheck = convertBytesToNumber(bbf.array(), columnDataTypes.get(column));
                    if (toCheck != null && predicate.test(toCheck)) { results.add(indexToCheck); }
                    bbf.clear();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    @Override
    public List<Integer> getMax(String column, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        if (!validationCheckForMinMax(column)) { return results; }
        
        try {
            RandomAccessFile fileInputStream = new RandomAccessFile(column+".store", "r");
            byte[] buffer = new byte[4];
            float maximum = Float.MIN_VALUE;
            float valueAtIndex;
            for (int indexToCheck: indexesToCheck) {
                fileInputStream.seek(indexToCheck* 4L);
                if(fileInputStream.read(buffer) != 4) {
                    System.out.println("Did not read 4 bytes when getting a number value from file.");
                }
                Object objectAtIndex = convertBytesToNumber(buffer, columnDataTypes.get(column));
                if (objectAtIndex == null) { continue; }
                valueAtIndex = (float) objectAtIndex; //cast to float. even if the underlying object is int, e.g. 24, it will cast to 24.0. it does NOT read the underlying bytes as float, still reads as int
                if (valueAtIndex == maximum) {
                    results.add(indexToCheck);
                } else if (valueAtIndex > maximum) {
                    results.clear();
                    results.add(indexToCheck);
                    maximum = valueAtIndex; //update new maximum
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    @Override
    public List<Integer> getMin(String column, List<Integer> indexesToCheck) {
        List<Integer> results = new ArrayList<>();
        if (!validationCheckForMinMax(column)) { return results; }

        try {
            RandomAccessFile fileInputStream = new RandomAccessFile(column+".store", "r");
            byte[] buffer = new byte[4];
            float minimum = Float.MAX_VALUE;
            float valueAtIndex;
            for (int indexToCheck: indexesToCheck) {
                fileInputStream.seek(indexToCheck* 4L);
                if(fileInputStream.read(buffer) != 4) {
                    System.out.println("Did not read 4 bytes when getting a number value from file.");
                }
                Object objectAtIndex = convertBytesToNumber(buffer, columnDataTypes.get(column));
                if (objectAtIndex == null) { continue; }
                valueAtIndex = (float) objectAtIndex; //cast to float. even if the underlying object is int, e.g. 24, it will cast to 24.0. it does NOT read the underlying bytes as float, still reads as int
                if (valueAtIndex == minimum) {
                    results.add(indexToCheck);
                } else if (valueAtIndex < minimum) {
                    results.clear();
                    results.add(indexToCheck);
                    minimum = valueAtIndex; //update new maximum
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    @Override
    public Object getValue(String column, int index) {
        try {
            if (isNotNumberDataType(column)) {
                //values are stored as string, separated by newlines
                //cannot skip index, must manually call nextLine() using simple Scanner class
                File file = new File(column+".store");
                Scanner sc = new Scanner(file);
                while (sc.hasNextLine() && index > 0) {
                    sc.nextLine();
                    index--;
                }

                String value = sc.nextLine();
                if (Objects.equals(value, "M")) { return null; }
                if (columnDataTypes.get(column) == STRING_DATATYPE) { return value; }
                else {
                    return LocalDateTime.parse(value, DateTimeFormatter.ofPattern(DTFORMATSTRING));
                }
            } else { //values are stored directly, each taking up 4 bytes. Can skip to index using RandomAccessFile
                RandomAccessFile fileInputStream = new RandomAccessFile(column+".store", "r");
                ByteBuffer bbf = ByteBuffer.allocate(4);
                fileInputStream.seek(index* 4L);
                if(fileInputStream.read(bbf.array()) != 4) {
                    System.out.println("Did not read 4 bytes when getting a number value from file.");
                }
                return convertBytesToNumber(bbf.array(), columnDataTypes.get(column));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void printHead(int until) {
        try {
            for (String column: columnHeaders) {
                System.out.print(column+": ");
                if (isNotNumberDataType(column)) {
                    Scanner sc = new Scanner(new File(column+".store"));
                    int temp = until;
                    while (temp > 0) {
                        System.out.print(sc.nextLine());
                        System.out.print(",");
                        temp--;
                    }
                } else {
                    FileInputStream fileInputStream = new FileInputStream(column+".store");
                    int temp = until;
                    while (temp > 0) {
                        System.out.print(convertBytesToNumber(fileInputStream.readNBytes(4), columnDataTypes.get(column)));
                        System.out.print(",");
                        temp--;
                    }
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Converts the byte array (should be of size 4) into either a float or integer based on the datatype passed in.
     * If the converted number is {@link Integer#MIN_VALUE} or {@link Float#isNaN()}, then return null.
     * @param buffer the byte array
     * @param dataType either {@link ColumnStoreAbstract#INTEGER_DATATYPE} or {@link ColumnStoreAbstract#FLOAT_DATATYPE}.
     * @return the number or null
     */
    private Object convertBytesToNumber(byte[] buffer, int dataType) {
        ByteBuffer bbf = ByteBuffer.allocate(4);
        bbf.put(buffer.clone());
        if (dataType == INTEGER_DATATYPE) {
            int result = bbf.getInt(0);
            if (result == Integer.MIN_VALUE) { return null; } //the design is such that Integer.MIN_VALUE is equivalent to null
            return result;
        } else if (dataType == FLOAT_DATATYPE) {
            float result = bbf.getFloat(0);
            if (Float.isNaN(result)) { return null; } //the design is such that Float.NaN is equivalent to null
            return result;
        } else {
            System.out.println("Wrong usage of this function (convertBytesToNumber). Should pass in only FLOAT or INTEGER dataType.");
            return null;
        }
    }

    /**
     * Append 4 bytes to the file representing a float.
     * @param fileOutputStream the outputStream of the file.
     * @param value integer to store
     */
    protected void handleStoreInteger(FileOutputStream fileOutputStream, int value) {
        ByteBuffer bbf = ByteBuffer.allocate(4);
        bbf.putInt(value);

        try {
            fileOutputStream.write(bbf.array());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Append 4 bytes to the file representing a float.
     * @param fileOutputStream the outputStream of the file
     * @param value float to store
     */
    protected void handleStoreFloat(FileOutputStream fileOutputStream, float value) {
        ByteBuffer bbf = ByteBuffer.allocate(4);
        bbf.putFloat(value);
        try {
            fileOutputStream.write(bbf.array());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Append 8 bytes to the file, representing a long.
     * @param fileOutputStream the outputStream of the file
     * @param value long to store
     */
    protected void handleStoreLong(FileOutputStream fileOutputStream, long value) {
        ByteBuffer bbf = ByteBuffer.allocate(8);
        bbf.putLong(value);
        try {
            fileOutputStream.write(bbf.array());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
