package common;

import common.utils.Utils;

/**
 * Created by semionn on 09.10.15.
 */
public class Column {
    private final String name;
    private final Type type;

    public static final int DECLARATION_BYTE_SIZE = Type.MAX_STRING_BYTE_SIZE + Type.DECLARATION_BYTE_SIZE;

    public Column(String name, Type type) {
        this.name = name.toUpperCase();
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
