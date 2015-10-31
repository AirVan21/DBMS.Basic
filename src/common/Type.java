package common;

import common.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by semionn on 09.10.15.
 */
public class Type {

    BaseType baseType;
    Map<String, Object> params;
    public static final int DECLARATION_BYTE_SIZE = 8; // 4 byte for type, 4 byte for params

    public static final int MAX_STRING_SIZE = 100;
    public static final int MAX_STRING_BYTE_SIZE = MAX_STRING_SIZE * Utils.getCharByteSize();

    int size;

    Type(BaseType baseType, Map<String, Object> params){
        this.baseType = baseType;
        this.params = params;
        switch (baseType) {
            case VARCHAR:
                size = 4 + (int)params.get("length") * Utils.getCharByteSize();
                break;
            case DOUBLE:
                size = Utils.getDoubleByteSize();
                break;
            case INT:
                size = Utils.getIntByteSize();
                break;
        }
    }

    public Type(BaseType baseType)
    {
        this(baseType, null);
    }

    public int getSize() {
        return size;
    }

    public String getName() { return baseType.name; }

    public static Type createType(String typeName) {
        for (BaseType baseType : BaseType.values()) {
            if (typeName.toUpperCase() == baseType.name) {
                return new Type(baseType, null);
            }
        }
        throw new IllegalArgumentException(String.format("Invalid type %s", typeName));
    }

    public static Type createType(String typeName, int length) {
        for (BaseType baseType : BaseType.values()) {
            if (typeName.toUpperCase().equals(baseType.name)) {
                Map<String, Object> params = new HashMap<>();
                params.put("length", length);
                return new Type(baseType, params);
            }
        }
        throw new IllegalArgumentException(String.format("Invalid type %s", typeName));
    }

    public Object castFromString(String value) {
        switch (baseType) {
            case VARCHAR:
                int maxlength = (int)params.get("length");
                if (value.length() > maxlength)
                    return value.substring(0, maxlength - 1);
                return value;
            case DOUBLE:
                return Double.parseDouble(value);
            case INT:
                return Integer.parseInt(value);
        }
        return null;
    }

}
