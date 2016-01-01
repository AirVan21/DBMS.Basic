package commands_runner;

import buffer_manager.HeapBufferManager;
import buffer_manager.IBufferManager;
import commands_runner.cursors.ICursor;
import commands_runner.indexes.AbstractIndex;
import commands_runner.indexes.TreeIndex;
import common.Column;
import common.ColumnSelect;
import common.conditions.Condition;
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
    public ICursor select(String tableName, List<ColumnSelect> columns, Conditions conditions) {
        if (!tablesMap.containsKey(tableName))
            throw new IllegalArgumentException(String.format("Table %s not found", tableName));

        Table table = tablesMap.get(tableName);
        // Uses conditions for complicated selects all over several tables
        ICursor cursor = bufferManager.getCursor(table, conditions);
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
    }

//    ICursor createCursor(List<Page> pages, Conditions conditions)
//    {
//        Map<String, Table> tables = new HashMap<>();
//        for (Page page : pages)
//        {
//            Table table = page.getTable();
//            if (!tables.containsKey(table.getName())){
//                tables.put(table.getName(), table);
//            }
//        }
//
//        ICursor cursor = null;
//
//        if (tables.getSize() == 1)
//            cursor = new SimpleCursor(pages, pages.get(0).getTable());
//
//        return cursor;
//    }

}
