package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import commands_runner.cursors.SimpleCursor;
import commands_runner.indexes.AbstractIndex;
import common.Column;
import common.Type;
import common.conditions.Conditions;
import common.table_classes.Record;
import common.table_classes.Table;


/**
 * Created by semionn on 30.10.15.
 */
public class TreeIndex extends AbstractIndex {

    Table table;
    Column column;
    LoadEngine loadEngine;
    BTreeDB bTree; //key column value and file offset
    BTreeIterator bTreeIterator;

    public TreeIndex(LoadEngine loadEngine, Table table, Column column) {
        this(loadEngine, table, column, createBTree(loadEngine, table, column));
    }

    private static BTreeDB createBTree(LoadEngine loadEngine,Table table, Column column) {
        return new BTreeDB(table, loadEngine, column.getType());
    }

    public void fillIndex() {
        int columnIndex = table.getColumnIndex(column);
        SimpleCursor cursor = new SimpleCursor(loadEngine, table);
        loadEngine.switchToTable(table);
        int c = 1;
        while (cursor.next()) {
            Record record = cursor.getRecord();
            int recordOffset = loadEngine.calcRecordOffset(cursor.getPageNum(), cursor.getRecordNum());
            bTree.put((Comparable<Object>) record.getColumnValue(columnIndex), recordOffset);
            System.out.println(c++);
        }
    }

    public TreeIndex(LoadEngine loadEngine, Table table, Column column, BTreeDB bTree) {
        this.table = table;
        this.column = column;
        this.loadEngine = loadEngine;
        this.bTree = bTree;
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

    @Override
    public IndexType getIndexType() {
        return IndexType.BTREE;
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public Type getKeyType() {
        return column.getType();
    }
}
