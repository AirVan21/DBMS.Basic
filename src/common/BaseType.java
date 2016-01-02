package common;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by semionn on 09.10.15.
 */
public enum BaseType {
    VARCHAR("VARCHAR", 0),
    DOUBLE("DOUBLE", 1),
    INT("INTEGER", 2);

    final String name;
    final int ID;

    BaseType(String name, int ID)
    {
        this.name = name;
        this.ID = ID;
    }

    public static BaseType createBaseType(int ID)
    {
        switch (ID) {
            case 0:
                return BaseType.VARCHAR;
            case 1:
                return BaseType.DOUBLE;
            case 2:
                return BaseType.INT;
        }
        throw new IllegalArgumentException(((Integer)ID).toString());
    }

    public int getTypeNumber() {
        return ID;
    }
}
