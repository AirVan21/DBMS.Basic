package common.table_classes;

import common.Condition;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class Page {
    Integer pageId; //?
    Integer pinCount;
    Boolean deleted;
    Boolean dirty;
    Boolean full;
    ArrayList<Record> records;

    public List<Record> getRecords(Condition condition) {
        List<Record> result = new ArrayList<>();
        for (Record record : records) {
            if (record.check(condition)) {
                result.add(record);
            }
        }
        return result;
    }

}
