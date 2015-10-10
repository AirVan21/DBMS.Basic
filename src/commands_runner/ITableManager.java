package commands_runner;
import common.Column;
import common.Condition;
import common.table_classes.Record;

import java.sql.*;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public interface ITableManager {
    void createTable(String tableName, List<Column> columns);
    void insert(String tableName, List<Column> columns, Condition assignments);
    List<Record> select(String tableName, List<Column> columns, Condition condition);
}
