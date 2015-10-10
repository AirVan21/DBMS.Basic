package buffer_manager;

import common.Column;
import common.Condition;
import common.table_classes.Page;

import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public interface IBufferManager {
    List<Page> getPages(Condition condition);

    void storeTable();

    void insert(List<Column> columns, Condition assignments);

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
