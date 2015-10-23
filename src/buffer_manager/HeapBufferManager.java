package buffer_manager;

import com.sun.corba.se.spi.orbutil.fsm.Input;
import common.Column;
import common.Condition;
import common.table_classes.MetaPage;
import common.table_classes.Page;
import common.table_classes.Table;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 09.10.15.
 */
public class HeapBufferManager extends AbstractBufferManager {
    List<Page> fullPages;
    List<Page> incompletePages;
    // HashMap for talbe + tablePath;
    Map<String, String> tableToPathMap;
    // XML file in future

    public HeapBufferManager(Integer maxPagesCount) {
        super(maxPagesCount);

        // Absolute path for root data base
        Path filePath = Paths.get("data//root_db.ndb");
        try {
            FileInputStream fileInput = new FileInputStream(filePath.toAbsolutePath().toString());
            ObjectInputStream objectInput = new ObjectInputStream(fileInput);
            // TODO: Reading hash-map from file
            objectInput.close();
        } catch (IOException e) {
            // Create root data base if it doesn't exist
            System.out.println("Created empty root_db.ndb (in heap)");
            tableToPathMap = new HashMap<String, String>();
        }
    }

    @Override
    public void createTable(String directory, String tableName, Table table) throws IOException {
        File tableFile = createTableFile(directory, tableName);
        defaultTableFilling(tableFile, table);
        // TODO: update cached sys.table file (INFO: sys.table file is XML (tableName + path structure)
        String pathToTable = Paths.get(directory + tableName).toAbsolutePath().toString();
        tableToPathMap.put(tableName, pathToTable);
        // TODO: update sys.tables file
    }

    @Override
    public void insert(Table table, List<Column> columns, Condition assignments) {
        // TODO: find table name through XML sys.table
        throw new NotImplementedException();
    }

    @Override
    public List<Page> getPages(Table table, Condition condition) {
        // TODO: pages which
        throw new NotImplementedException();
    }

    /*
        Creates new File for table "fileName" in ../data/
     */
    private File createTableFile(String directory, String tableName) {
        Path filePath = Paths.get(directory + tableName);
        File tableFile = new File(filePath.toAbsolutePath().toString());

        // Preventing table re-creation
        try {
            tableFile.createNewFile();
        } catch (IOException alreadyExistException) {
            System.out.println("Table with name = " + filePath.normalize().toString() + " already exist!");
            alreadyExistException.printStackTrace();
        }

        return tableFile;
    }

    /*
        Creates serializable page with meta-info and writs int to tableFile
     */
    private void defaultTableFilling(File tableFile, Table table) throws IOException {
        FileOutputStream fileOutput = new FileOutputStream(tableFile);
        ObjectOutputStream objectOutput = new ObjectOutputStream(fileOutput);
        // Using Serializable MataPage representation
        MetaPage defaultPage = new MetaPage(table.getColumns());
        objectOutput.writeObject(defaultPage);
        objectOutput.flush();
        objectOutput.close();
    }
}
