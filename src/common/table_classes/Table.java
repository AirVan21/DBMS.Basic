package common.table_classes;

import common.Column;

import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class Table {
    String name;
    List<Column> columns;

    public Table(String name, List<Column> columns){
        this.name = name;
        this.columns = columns;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
