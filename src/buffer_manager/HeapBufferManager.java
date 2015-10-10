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
    List<Page> incompletePages;

    @Override
    public void insert(List<Column> columns, Condition assignments) {
        throw new NotImplementedException();
    }

    @Override
    public void storeTable() {
        throw new NotImplementedException();
        /*TODO: generate a table with filled header
                register in sys.table and flush it*/
    }

    public HeapBufferManager(Integer maxPagesCount, String filePath, Table table) {
        super(maxPagesCount, filePath, table);
    }


    @Override
    public List<Page> getPages(Condition condition) {
        throw new NotImplementedException();
    }
}
