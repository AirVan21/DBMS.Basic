package commands_runner;

import buffer_manager.HeapBufferManager;
import buffer_manager.IBufferManager;
import commands_runner.cursors.ICursor;
import commands_runner.indexes.AbstractIndex;
import commands_runner.indexes.btree.BTreeSerializer;
import commands_runner.indexes.btree.TreeIndex;
import common.Column;
import common.ColumnSelect;
import common.FromClause;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 09.10.15.
 */
public class TableManager implements ITableManager, AutoCloseable {
    Map<String, Table> tablesMap = new HashMap<>();
    final int maxPagesCount;
    final String dirPath;
    final IBufferManager bufferManager;

    public TableManager(int maxPagesCount, String dirPath) {
        this.maxPagesCount = maxPagesCount;
        this.dirPath = dirPath;
        this.bufferManager = new HeapBufferManager(maxPagesCount, dirPath);
    }

    @Override
    public void flushAllTables() {
        bufferManager.flushAllData();
    }

    @Override
    public boolean createTable(String tableName, List<Column> columns) {
        String fileName = generateFileName(tableName);
        Table newTable = new Table(tableName, fileName, columns);
        if (!bufferManager.createTable(dirPath, newTable))
            return false;

        tablesMap.put(tableName, newTable);
        return true;
    }

    String generateFileName(String tableName) {
        return tableName + ".ndb";
    }

    @Override
    public void insert(String tableName, Conditions assignments) {
        if (!tablesMap.containsKey(tableName))
            throw new IllegalArgumentException(String.format("Table %s not found", tableName));

        Table table = tablesMap.get(tableName);
        Record record = new Record(table.getColumns(), assignments);
        try {
            bufferManager.insert(table, record);
        } catch (QueryException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ICursor select(FromClause fromClause, List<ColumnSelect> columns, Conditions conditions) {
//        if (!tablesMap.containsKey(fromClause))
//            throw new IllegalArgumentException(String.format("Table %s not found", fromClause));

//        Table table = tablesMap.get(fromClause);
        // Uses conditions for complicated selects all over several tables
        ICursor cursor = null;
        try {
            cursor = bufferManager.getCursor(fromClause, conditions, true);
        } catch (QueryException e) {
            e.printStackTrace();
        }
        return cursor;
    }

    @Override
    public void loadTables() {
        tablesMap = bufferManager.loadTables();
    }

    @Override
    public void createIndex(String tableName, Column column) {
        if (!tablesMap.containsKey(tableName))
            throw new IllegalArgumentException(String.format("Table %s not found", tableName));
        Table table = tablesMap.get(tableName);
        if (table.getColumnIndex(column) < 0)
            throw new IllegalArgumentException(String.format("Column %s not found", column.getName()));
        AbstractIndex index = new TreeIndex(bufferManager.getLoadEngine(), table, column);
        table.setIndex(index);
        index.fillIndex();
        bufferManager.updateTableInfo(table);
    }

    @Override
    public Table getTable(String tableName) {
        if (!tablesMap.containsKey(tableName))
            return null;
        return tablesMap.get(tableName);
    }

    @Override
    public void close() throws Exception {
        flushAllTables();
        for (Table table : tablesMap.values()) {
            AbstractIndex index = table.getIndex();
            if (index != null)
                switch (index.getIndexType()) {
                    case BTREE:
                        BTreeSerializer.serialize((TreeIndex) index, bufferManager.getLoadEngine(), table.getIndexFileName());
                        break;
                    case HASH:
                        break;
                }
        }
    }

    @Override
    public int delete(String tableName, Conditions conditions) {
        if (!tablesMap.containsKey(tableName))
            throw new IllegalArgumentException(String.format("Table %s not found", tableName));
        Table table = tablesMap.get(tableName);
        try {
            return bufferManager.delete(table, conditions);
        } catch (QueryException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
