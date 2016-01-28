package common.conditions;

import common.Column;
import common.ColumnSelect;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.List;

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

    public boolean check(Record record, List<ColumnSelect> columnsSelect)
    {
        Comparable<Object> recValue = null;
        if (columnsSelect == null) {
            int columnIndex = table.getColumnIndex(column);
            recValue = (Comparable<Object>) record.getColumnValue(columnIndex);
        } else {
            for (int i = 0; i < columnsSelect.size(); i++) {
                if (columnsSelect.get(i).getTable() == table &&
                        columnsSelect.get(i).getСolumn() == column) {
                    recValue = (Comparable<Object>) record.getColumnValue(i);
                    break;
                }
            }
        }
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
