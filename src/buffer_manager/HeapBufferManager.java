package buffer_manager;

import common.Column;
import common.Condition;
import common.table_classes.MetaPage;
import common.table_classes.Page;
import common.table_classes.Table;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class HeapBufferManager extends AbstractBufferManager {
    List<Page> fullPages;
    List<Page> incompletePages;

    @Override
    public void createTable(String fileName, Table table) throws IOException {
        File tableFile = createTableFile(fileName);
        defaultTableFilling(tableFile, table);
        // TODO: update sys.tables file
        // TODO: update cached sys.table file (INFO: sys.table file is XML (tableName + path structure)
    }

    @Override
    public void insert(Table table, List<Column> columns, Condition assignments) {
        // TODO: find table name through XML sys.table
        throw new NotImplementedException();
    }

    public HeapBufferManager(Integer maxPagesCount) {
        super(maxPagesCount);
    }

    @Override
    public List<Page> getPages(Table table, Condition condition) {
        // TODO: pages which
        throw new NotImplementedException();
    }

    /*
        Creates new File for table "fileName" in ../data/
     */
    private File createTableFile(String fileName) {
        Path filePath = Paths.get(fileName);
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
