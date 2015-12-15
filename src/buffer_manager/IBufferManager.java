package buffer_manager;

import commands_runner.cursors.ICursor;
import common.Column;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 09.10.15.
 */
public interface IBufferManager {
    ICursor getCursor(Table table, Conditions conditions);

    void insert(Table table, Record record) throws QueryException;

    void createTable(String directory, Table table);

    Map<String,Table> loadTables();

    void flushAllData();

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
