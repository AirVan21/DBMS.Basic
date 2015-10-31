package common.conditions;

import common.Column;
import common.table_classes.Table;

/**
 * Created by semionn on 31.10.15.
 */
public class Condition {
    Table table;
    Column column;
    ComparisonType comparisonType;
    Object value;

    public Condition(Table table, Column column, ComparisonType comparisonType, Object value) {
        this.table = table;
        this.column = column;
        this.comparisonType = comparisonType;
        this.value = value;
    }

    public Table getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }
}
