package commands_runner.indexes.btree;

/**
 * Created by semionn on 01.01.16.
 */
public enum IndexType {
    BTREE("BTREE"),
    HASH("HASH");

    private final String name;

    IndexType(String s) {
        name = s;
    }

    @Override
    public String toString() {
        return name;
    }
}
