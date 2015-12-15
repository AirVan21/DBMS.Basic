package commands_runner;

import buffer_manager.HeapBufferManager;
import buffer_manager.IBufferManager;
import commands_runner.cursors.ICursor;
import common.Column;
import common.ColumnSelect;
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
public class TableManager implements ITableManager {
    Map<String, Table> tablesMap = new HashMap<>();
    final int maxPagesCount;
    final String dirPath;
    final IBufferManager bufferManager;

    public TableManager(int maxPagesCount, String dirPath) {
        this.maxPagesCount = maxPagesCount;
        this.dirPath = dirPath;
        this.bufferManager = new HeapBufferManager(maxPagesCount);
    }

    @Override
    public void flushAllTables() {
        bufferManager.flushAllData();
    }

    @Override
    public void createTable(String tableName, List<Column> columns) {
        String fileName = generateFileName(tableName);
        Table newTable = new Table(tableName, fileName, columns);
        tablesMap.put(tableName, newTable);
        bufferManager.createTable(dirPath, newTable);
    }

    String generateFileName(String tableName) {
        return tableName + ".ndb";
    }

    @Override
    public void insert(String tableName, List<Column> columns, Conditions assignments) {
        if (!tablesMap.containsKey(tableName))
            throw new IllegalArgumentException(String.format("Table %s not found", tableName));

        Table table = tablesMap.get(tableName);
        Record record = new Record(columns, assignments);
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
    public Table getTable(String tableName) {
        if (!tablesMap.containsKey(tableName))
            return null;
        return tablesMap.get(tableName);
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
//        if (tables.size() == 1)
//            cursor = new SimpleCursor(pages, pages.get(0).getTable());
//
//        return cursor;
//    }

}
