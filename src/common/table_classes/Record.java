package common.table_classes;

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

    public Object getColumnValue(int columnIndex) {
        return values.get(columnIndex);
    }
}
