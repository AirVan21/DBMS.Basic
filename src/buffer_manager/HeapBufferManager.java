package buffer_manager;

import common.Column;
import common.conditions.Conditions;
import common.table_classes.MetaPage;
import common.table_classes.Page;
import common.table_classes.Table;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import common.xml.XMLBuilder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


/**
 * Created by semionn on 09.10.15.
 */
public class HeapBufferManager extends AbstractBufferManager {

    private XMLBuilder sysTable;
    private LoadEngine loadEngine;

    public HeapBufferManager(Integer maxPagesCount) {
        super(maxPagesCount);
        // Absolute path for root data base
        Path filePath = Paths.get("data//root_db.ndb");
        sysTable = new XMLBuilder(filePath.toAbsolutePath().toString());
        loadEngine = new LoadEngine(maxPagesCount);
    }

    @Override
    public void createTable(String directory, Table table) {
        String tableName = table.getName();
        Path pathToTable = Paths.get(directory + table.getFileName());
        if (!sysTable.isExist(tableName)) {
            // Creating new table
            loadEngine.switchToTable(pathToTable.toAbsolutePath().toString());
            loadEngine.writeMetaPage(table);
            // Modify Sys Table
            sysTable.addRecord(tableName, pathToTable.toString());
            sysTable.storeXMLDocument();

        } else {
            System.out.println("Table name duplication!");
            // Own exception should be thrown
        }

        System.out.println(table.getName());
    }

    @Override
    public void insert(Table table, List<Column> columns, Conditions assignments) {
        // TODO: find table name through XML sys.table
        throw new NotImplementedException();
    }

    @Override
    public List<Page> getPages(Table table, Conditions conditions) {
        // Optimize this
        if (sysTable.isExist(table.getName())) {
            String tablePath = sysTable.getTablePath(table.getName());
            loadEngine.switchToTable(tablePath);
            loadEngine.readMetaPage(table);
        } else {
            System.out.println("Not such data base file!");
        }
        return null;
    }
}
