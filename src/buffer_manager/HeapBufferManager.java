package buffer_manager;

import commands_runner.cursors.ICursor;
import commands_runner.cursors.IndexCursor;
import commands_runner.cursors.ProjectCursor;
import commands_runner.cursors.SimpleCursor;
import commands_runner.indexes.AbstractIndex;
import commands_runner.indexes.btree.BTreeSerializer;
import commands_runner.indexes.btree.IndexType;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Record;
import common.table_classes.Table;
import common.xml.SysInfoRecord;
import common.xml.XMLBuilder;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by semionn on 09.10.15.
 */
public class HeapBufferManager extends AbstractBufferManager {

    private XMLBuilder sysTable;
    private LoadEngine loadEngine;

    public HeapBufferManager(Integer maxPagesCount, String dbFolder) {
        super(maxPagesCount);
        // Absolute path for root data base
        DATA_ROOT_DB_FILE = Paths.get(dbFolder + DATA_ROOT_DB_NAME);
        sysTable = new XMLBuilder(DATA_ROOT_DB_FILE.toAbsolutePath().toString());
        loadEngine = new LoadEngine(maxPagesCount);
    }

    @Override
    public boolean createTable(String directory, Table table) {
        String tableName = table.getName();
        Path pathToTable = Paths.get(directory + table.getFileName());
        table.setFileName(pathToTable.toAbsolutePath().toString());
        if (sysTable.isExist(tableName)) {
            System.out.println("Table '" + tableName + "' duplication!");
            return false;
        }

        File file = new File(directory);
        if (!file.exists()) {
            file.mkdirs();
        }
        // Creating new table
        loadEngine.switchToNewTable(table);
        loadEngine.writeMetaPage(table);

        // Modify Sys Table
        String indexType = "";
        if (table.getIndex() != null)
            indexType = table.getIndex().getIndexType().toString();
        sysTable.addRecord(new SysInfoRecord(tableName, pathToTable.toString(), indexType, table.getIndexFileName()));
        sysTable.storeXMLDocument();

        return true;
    }

    @Override
    public Map<String, Table> loadTables() {
        Map<String, Table> result = new HashMap<>();
        List<SysInfoRecord> content = sysTable.loadContents();
        for (SysInfoRecord item : content) {
            Table table = new Table(item.tableName, item.tablePath, null);
            loadEngine.switchToTable(table);
            if (item.indexType.equals(IndexType.BTREE.toString()))
                table.setIndex(BTreeSerializer.deserialize(item.indexPath, loadEngine, table));
            result.put(item.tableName, table);
        }
        return result;
    }

    @Override
    public void updateTableInfo(Table table) {
        sysTable.updateTableInfo(table);
    }

    @Override
    public void flushAllData() {
        loadEngine.flushAllData();
        sysTable.storeXMLDocument();
    }

    @Override
    public void insert(Table table, Record record) throws QueryException {
        if (sysTable.isExist(table.getName())) {
            loadEngine.switchToTable(table);
            loadEngine.storeRecordInPage(record);
        } else {
            throw new QueryException("Trying insert in table which do not exist!");
        }
    }

    @Override
    public ICursor getCursor(Table table, Conditions conditions) {
        if (sysTable.isExist(table.getName())) {
            AbstractIndex index = table.getIndex();
            ICursor cursor;
            if (index != null) {
                cursor = new IndexCursor(index, conditions);
            }
            else {
                cursor = new SimpleCursor(loadEngine, table);
            }
            if (conditions != null)
            {
                return new ProjectCursor(cursor, conditions, table);
            }
            return cursor;
        } else {
            System.out.println("Not such data base file!");
        }
        return null;
    }

    public LoadEngine getLoadEngine() {
        return loadEngine;
    }
}
