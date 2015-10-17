package buffer_manager;

import common.Column;
import common.Condition;
import common.table_classes.Page;
import common.table_classes.Table;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class HeapBufferManager extends AbstractBufferManager {
    List<Page> fullPages;

    @Override
    public void createTable(String fileName, Table table) {
        // TODO: create file for current table
        // TODO: update sys.tables file
        // TODO: update cached sys.table file (INFO: sys.table file is XML (tableName + path structure)
    }

    List<Page> incompletePages;

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
}
