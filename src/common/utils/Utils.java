package common.utils;

/**
 * Created by semionn on 09.10.15.
 */
public class Utils {
    public static int getCharByteSize(){
        return 2;
    }

    public static int getIntByteSize(){
        //can be used to determine x86 or x64: System.getProperty("os.arch");
        return 8;
    }

    public static int getDoubleByteSize(){
        return 16;
    }
}
