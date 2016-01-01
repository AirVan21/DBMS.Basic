package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import common.Column;
import common.table_classes.Table;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by semionn on 01.01.16.
 */
public class BTreeSerializer {
    public static TreeIndex deserialize(String fileName, LoadEngine loadEngine, Table table) {
        BTreeDB bTree = new BTreeDB(table);
        Column column = null;
        //TODO: read data from file
        return new TreeIndex(loadEngine, table, column, bTree);
    }

    public static void serialize(TreeIndex treeIndex) {
        throw new NotImplementedException();
    }
}
