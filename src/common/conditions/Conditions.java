package common.conditions;


import common.Column;
import common.NullObject;
import common.table_classes.Record;
import common.table_classes.Table;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

/**
 * Created by semionn on 09.10.15.
 */
public class Conditions {
    private List<Condition> values;

    public Conditions(List<Condition> values) {
        this.values = values;
    }

    public Conditions() {
        this(new ArrayList<Condition>());
    }

    public void addValue(Condition condition){
        values.add(condition);
    }

    public List<Condition> getValues() {
        return values;
    }

    public Object getColumnValue(final String columnName) {
        return CollectionUtils.find(values, new Predicate() {
            public boolean evaluate(Object a) {
                return ((Condition) a).getColumn().getName() == columnName;
            }
        });
    }


    private boolean columnExists(final Table table, final Column column) {
        return CollectionUtils.exists(values, new Predicate() {
            public boolean evaluate(Object a) {
                return ((Condition) a).getColumn() == column &&
                       ((Condition) a).getTable() == table;
            }
        });
    }

    public boolean check(Record record)
    {
        for (Condition condition : values)
        {
            if (!condition.check(record))
                return false;
        }
        return true;
    }

}
