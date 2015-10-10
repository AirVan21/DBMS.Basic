package common.table_classes;

import buffer_manager.IBufferManager;
import common.Condition;

import java.util.*;

/**
 * Created by semionn on 09.10.15.
 */
public class Record {
    Integer rid;
    List<Object> values;

    Boolean check(Condition condition) throws IllegalArgumentException {
        if (!condition.getNormalized()){
            throw new IllegalArgumentException("condition don't normalized!");
        }
        Iterator<Map.Entry<String, Object>> entryIterator = condition.getValues().entrySet().iterator();
        for (int i = 0; entryIterator.hasNext(); i++) {
            if (!values.get(i).equals(entryIterator.next().getValue()))
                return false;
        }
        return true;
    }
}
