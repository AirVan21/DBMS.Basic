package buffer_manager;

import common.Column;
import common.Condition;
import common.table_classes.Page;
import common.table_classes.Table;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public interface IBufferManager {
    List<Page> getPages(Table table, Condition condition);

    void insert(Table table, List<Column> columns, Condition assignments);

    void createTable(String fileName, Table table) throws IOException;

    //Main
    // |
    //->SELECT/INSERT Condition
    // |
    //Table Manager
    // |
    // Buffer Manager
    // |
    // RAW_DATA

}
