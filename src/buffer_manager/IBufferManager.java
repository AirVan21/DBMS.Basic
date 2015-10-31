package buffer_manager;

import common.Column;
import common.conditions.Conditions;
import common.table_classes.Page;
import common.table_classes.Table;

import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public interface IBufferManager {
    List<Page> getPages(Table table, Conditions conditions);

    void insert(Table table, List<Column> columns, Conditions assignments);

    void createTable(String directory, Table table);

    //Main
    // |
    //->SELECT/INSERT Conditions
    // |
    //Table Manager
    // |
    // Buffer Manager
    // |
    // RAW_DATA

}
