package commands_runner;

import buffer_manager.HeapBufferManager;
import buffer_manager.IBufferManager;
import common.Column;
import common.Condition;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;

import javax.swing.*;
import java.io.IOException;
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

    public TableManager(Integer maxPagesCount, String dirPath) {
        this.maxPagesCount = maxPagesCount;
        this.dirPath = dirPath;
        this.bufferManager = new HeapBufferManager(maxPagesCount);
    }

    @Override
    public void createTable(String tableName, List<Column> columns) {
        Table newTable = new Table(tableName, columns);
        tablesMap.put(tableName, newTable);
        String fileName = generateFileName(tableName);

        // TODO: rewrite awful exception handling routing
        try {
            bufferManager.createTable(fileName, newTable);
        } catch (IOException e) {
            System.out.println("Something wrong with table creation!");
        }
    }

    String generateFileName(String tableName) {
        return dirPath + tableName + ".ndb";
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
