package commands_runner.indexes.btree;

/**
 * Created by semionn on 01.01.16.
 */
public enum IndexType {
    BTREE("BTREE", 0),
    HASH("HASH", 1);

    private final String name;
    private final int num;

    IndexType(String name, int num) {
        this.name = name;
        this.num = num;
    }

    @Override
    public String toString() {
        return name;
    }

    public int getNum() {
        return num;
    }
}
