package commands_runner;
import commands_runner.cursors.ICursor;
import common.Column;
import common.ColumnSelect;
import common.FromClause;
import common.conditions.Conditions;
import common.table_classes.Table;

import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public interface ITableManager {
    boolean createTable(String tableName, List<Column> columns);
    void insert(String tableName, Conditions assignments);
    ICursor select(FromClause fromClause, List<ColumnSelect> columns, Conditions conditions);
    int delete(String tableName, Conditions conditions);

    void loadTables();

    void createIndex(String tableName, Column column);

    void flushAllTables();


    Table getTable(String tableName);
}
