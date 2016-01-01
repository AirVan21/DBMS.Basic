package commands_runner.indexes;

import buffer_manager.LoadEngine;
import commands_runner.cursors.ICursor;
import commands_runner.cursors.SimpleCursor;
import common.Column;
import common.conditions.Conditions;
import common.table_classes.Record;
import common.table_classes.Table;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * Created by semionn on 30.10.15.
 */
public class TreeIndex extends AbstractIndex {

    Table table;
    Column column;
    LoadEngine loadEngine;
    BTree<Comparable<Object>, Integer> bTree; //key column value and file offset
    BTreeIterator bTreeIterator;

    public TreeIndex(LoadEngine loadEngine, Table table, Column column) {
        this.table = table;
        this.column = column;
        this.loadEngine = loadEngine;
        bTree = new BTree<>(table);
        int columnIndex = table.getColumnIndex(column);
        SimpleCursor cursor = new SimpleCursor(loadEngine, table);
        loadEngine.switchToTable(table);
        while (cursor.next()) {
            Record record = cursor.getRecord();
            int recordOffset = loadEngine.calcRecordOffset(cursor.getPageNum(), cursor.getRecordNum());
            bTree.put((Comparable<Object>) record.getColumnValue(columnIndex), recordOffset);
        }
    }

    @Override
    public void setIterator(Conditions conditions) {
        bTreeIterator = new BTreeIterator(this, bTree, loadEngine, conditions);
    }

    @Override
    public boolean next() {
        return bTreeIterator.next();
    }

    @Override
    public Record getRecord() {
        return bTreeIterator.getRecord();
    }

    public Column getColumn() {
        return column;
    }
}
