package commands_runner;

import buffer_manager.HeapBufferManager;
import buffer_manager.IBufferManager;
import common.Column;
import common.Condition;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 09.10.15.
 */
public class TableManager implements ITableManager {
    Map<String, Table> tablesMap = new HashMap<>();
    final Integer maxPagesCount;
    final String dirPath;
    final IBufferManager bufferManager;

    TableManager(Integer maxPagesCount, String dirPath) {
        this.maxPagesCount = maxPagesCount;
        this.dirPath = dirPath;
        this.bufferManager = new HeapBufferManager(maxPagesCount);
    }

    @Override
    public void createTable(String tableName, List<Column> columns) {
        Table newTable = new Table(tableName, columns);
        tablesMap.put(tableName, newTable);
        String fileName = generateFileName(tableName);
        bufferManager.createTable(fileName, newTable);
    }

    String generateFileName(String tableName) {
        return tableName+".ndb";
    }

    @Override
    public void insert(String tableName, List<Column> columns, Condition assignments) {
        if (!tablesMap.containsKey(tableName))
            throw new IllegalArgumentException(String.format("Table %s not found", tableName));

        Table table = tablesMap.get(tableName);
        bufferManager.insert(table, columns, assignments);
    }

    @Override
    public List<Record> select(String tableName, List<Column> columns, Condition condition) {
        if (!tablesMap.containsKey(tableName))
            throw new IllegalArgumentException(String.format("Table %s not found", tableName));

        Table table = tablesMap.get(tableName);
        condition.normalize(table);
        // Uses condition for complicated selects all over several tables
        List<Page> pages = bufferManager.getPages(table, condition);
        List<Record> result = new ArrayList<>();
        for (Page page : pages) {
            result.addAll(page.getRecords(condition));
        }
        return result;
    }

}
