package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import commands_runner.cursors.SimpleCursor;
import commands_runner.indexes.AbstractIndex;
import common.Column;
import common.Type;
import common.conditions.Conditions;
import common.table_classes.Record;
import common.table_classes.Table;

import java.io.*;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by semionn on 30.10.15.
 */
public class TreeIndex extends AbstractIndex {

    Table table;
    Column column;
    LoadEngine loadEngine;
    BTreeDB bTree; //key column value and file offset
    BTreeIterator bTreeIterator;
    List<Integer> leafNodes;
    boolean filled;

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
//            if (c > 190 && bTree.get((Comparable<Object>) (Object) 1850) == null)
//                break;
            System.out.println(c++);

//            try {
//                File file = new File("dataTree/index_"+c+".txt");
//                // creates the file
//                file.createNewFile();
//                // creates a FileWriter Object
//                FileWriter writer = new FileWriter(file);
//                // Writes the content to the file
//                writer.write(bTree.toString());
//                writer.flush();
//                writer.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            bTree.toString();
//            if (c > 2)
//            try {
//                File file = new File("dataTree/index_"+c+".txt");
//                //Creates a FileReader Object
//                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//                    String fileLine, currLine;
//                    String[] bTreeText = bTree.toString().split("\n");
//                    int i = 0;
//                    while (!(fileLine = br.readLine()).equals("")) {
//                        currLine = bTreeText[i++];
//                        if (!currLine.equals(fileLine))
//                            break;
//
//                    }
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
        fillLeafNodes(bTree.getRoot());
        filled = true;
    }

    public TreeIndex(LoadEngine loadEngine, Table table, Column column, BTreeDB bTree) {
        this.table = table;
        this.column = column;
        this.loadEngine = loadEngine;
        this.bTree = bTree;
        this.leafNodes = new ArrayList<>();
        filled = false;
    }

    @Override
    public void setIterator(Conditions conditions) {
        if (!filled) {
            fillLeafNodes(bTree.getRoot());
            filled = true;
        }
        bTreeIterator = new BTreeIterator(this, bTree, loadEngine, conditions, leafNodes);
    }

    public void fillLeafNodes(Node node) {
        if (node.currLen > 0)
            if (node.children[0].nextID == -1)
                leafNodes.add(node.getID());
            else
                for (int i = 0; i < node.currLen; i++) {
                    Entry entry = node.children[i];
                    if (entry != null)
                        fillLeafNodes((Node) loadEngine.getTreeIndexPageFromBuffer(entry.nextID, bTree.getOrder(), bTree.getKeyType()));
                }
    }


    @Override
    public boolean next() {
        if (bTreeIterator.next())
            return true;
//        filled = false;
        return false;
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

    public void setFilled(boolean filled) {
        this.filled = filled;
    }

    @Override
    public String toString() {
        return bTree.toString();
    }
}
