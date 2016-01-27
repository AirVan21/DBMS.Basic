package common.table_classes;

import common.Column;
import common.conditions.Condition;
import common.conditions.Conditions;

import java.util.*;

/**
 * Created by semionn on 09.10.15.
 */
public class Record {
    Integer rid;
    public List<Object> values;

    /*Boolean check(Conditions conditions) throws IllegalArgumentException {
        Iterator<Condition> conditionIterator = conditions.getValues().iterator();
        for (int i = 0; conditionIterator.hasNext(); i++) {
            if (!values.get(i).equals(conditionIterator.next().getValue()))
                return false;
        }
        return true;
    }*/

    public Record(List<Column> columns, Conditions assignment) {
        values = new ArrayList<>();
        for (Column column : columns) {
            values.add(((Condition)assignment.getColumnValue(column.getName())).getValue());
        }
    }

    public Record(List<Object> values) {
        this.values = values;
    }

    public Object getColumnValue(int columnIndex) {
        return values.get(columnIndex);
    }

    public List<Object> getColumnsValues(List<Integer> columnsIndexes) {
        List<Object> result = new ArrayList<>();
        for (Integer columnIndex : columnsIndexes) {
            result.add(values.get(columnIndex));
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object obj : values) {
            stringBuilder.append(obj.toString());
        }
        return stringBuilder.toString();
    }
}
