package commands_runner;
import commands_runner.cursors.ICursor;
import common.Column;
import common.ColumnSelect;
import common.conditions.Conditions;
import common.table_classes.Table;

import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public interface ITableManager {
    void createTable(String tableName, List<Column> columns);
    void insert(String tableName, List<Column> columns, Conditions assignments);
    ICursor select(String tableName, List<ColumnSelect> columns, Conditions conditions);

    Table getTable(String tableName);
}
