package common;

/**
 * Created by semionn on 09.10.15.
 */
public enum BaseType {
    VARCHAR("VARCHAR"),
    DOUBLE("DOUBLE"),
    INT("INTEGER");

    final String name;

    BaseType(String name)
    {
        this.name = name;
    }
}
