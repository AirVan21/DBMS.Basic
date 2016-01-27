package common;

import common.table_classes.Table;

/**
 * Created by semionn on 31.10.15.
 */
public class ColumnSelect {
    Column сolumn;
    Table table;

    public ColumnSelect(Table table, Column column) {
        this.table = table;
        this.сolumn = column;
    }

    public Column getСolumn() {
        return сolumn;
    }

    public Table getTable() {
        return table;
    }
}
