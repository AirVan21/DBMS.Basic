package common.conditions;

import common.Column;
import common.table_classes.Record;
import common.table_classes.Table;

/**
 * Created by semionn on 31.10.15.
 */
public class Condition {
    Table table;
    Column column;
    ComparisonType comparisonType;
    Comparable<Object> value;

    public Condition(Table table, Column column, ComparisonType comparisonType, Object value) {
        this.table = table;
        this.column = column;
        this.comparisonType = comparisonType;
        this.value = (Comparable<Object>) value;
    }

    public Table getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public Comparable<Object> getValue() {
        return value;
    }

    public ComparisonType getComparisonType() {
        return comparisonType;
    }

    public boolean check(Record record)
    {
        int columnIndex = table.getColumnIndex(column);
        Comparable<Object> recValue = (Comparable<Object>)record.getColumnValue(columnIndex);

        switch (comparisonType) {
            case LESS:
                return recValue.compareTo(value) < 0;
            case LESSEQ:
                return recValue.compareTo(value) <= 0;
            case EQUAL:
                return recValue.compareTo(value) == 0;
            case GREATEQ:
                return recValue.compareTo(value) >= 0;
            case GREAT:
                return recValue.compareTo(value) > 0;
        }
        return false;
    }

}
