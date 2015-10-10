package common;


import common.table_classes.Table;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 09.10.15.
 */
public class Condition {
    private Map<String, Object> values;
    private Boolean normalized = false;

    Condition(Map<String, Object> values) {
        this.values = values;
    }

    void addValue(String columnName, Object value){
        values.put(columnName, value);
    }

    public Boolean getNormalized() {
        return normalized;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public void normalize(Table table) {
        List<Column> tableColumns = table.getColumns();
        Map<String, Object> tempMap = new LinkedHashMap<String, Object>();
        for (Column column : tableColumns) {
            String columnName = column.getName();
            if (values.containsKey(columnName)) {
                tempMap.put(columnName, values.get(columnName));
            } else {
                tempMap.put(columnName, new NullObject());
            }
        }
        values = tempMap;
        normalized = true;
    }

}
