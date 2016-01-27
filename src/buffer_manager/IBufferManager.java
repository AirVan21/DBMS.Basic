package buffer_manager;

import commands_runner.cursors.ICursor;
import common.ColumnSelect;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 09.10.15.
 */
public interface IBufferManager {
    ICursor getCursor(Table table, List<ColumnSelect> selectColumns, Conditions conditions) throws QueryException ;

    void insert(Table table, Record record) throws QueryException;

    boolean createTable(String directory, Table table);

    Map<String,Table> loadTables();

    void flushAllData();

    LoadEngine getLoadEngine();

    void updateTableInfo(Table table);

    int delete(Table table, Conditions conditions) throws QueryException;

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
