package common.utils;

import common.Type;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by semionn on 09.10.15.
 */
public class Utils {
    public static int getCharByteSize(){
        return 2;
    }

    public static int getIntByteSize(){
        //can be used to determine x86 or x64: System.getProperty("os.arch");
        return 4;
    }

    public static int getDoubleByteSize(){
        return 8;
    }

    public static int getMaxObjectSize() {
        return Collections.max(Arrays.asList(getIntByteSize(),
                                            getDoubleByteSize(),
                                            Type.MAX_STRING_BYTE_SIZE));
    }

    public static void writeObjectToFile(Object value, Type type, RandomAccessFile file) throws IOException {
        switch (type.getBaseType()) {
            case VARCHAR:
                long pos = file.getFilePointer();
                file.write(((String) value).getBytes("UTF-16"));
                file.seek(pos + Type.MAX_STRING_BYTE_SIZE);
                break;
            case DOUBLE:
                file.writeDouble((double) value);
                break;
            case INT:
                file.writeInt((int) value);
                break;
        }
    }

    public static Object readObjectFromFile(Type type, RandomAccessFile file) throws IOException {
        switch (type.getBaseType()) {
            case VARCHAR:
                int size = file.readInt();
                byte[] string = new byte[size];
                file.read(string, 0, size);
                return new String(string, "UTF-16").trim();
            case DOUBLE:
                return file.readDouble();
            case INT:
                return file.readInt();
        }
        return null;
    }
}
