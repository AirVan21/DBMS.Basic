package common;

/**
 * Created by semionn on 09.10.15.
 */
public class Column {
    private final String name;
    private final Type type;

    public Column(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }
}
