package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import common.Column;
import common.Type;
import common.table_classes.Page;
import common.table_classes.Table;
import common.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by semionn on 01.01.16.
 */
public class BTreeSerializer {
    public static TreeIndex deserialize(String fileName, LoadEngine loadEngine, Table table) {
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "rw");

            int indexType = file.readInt();
            if (indexType != IndexType.BTREE.getNum())
                throw new IllegalArgumentException();

            int treeOrder = file.readInt();
            int rightOrder = BTreeSerializer.ENTRY_COUNT;
            if (rightOrder % 2 != 0)
                rightOrder++;
            if (treeOrder != rightOrder)
                throw new IllegalArgumentException();

            int rootID = file.readInt();
            int height = file.readInt();
            int N      = file.readInt();
            int counter = file.readInt();

            int columnNameLength = file.readInt();
            byte[] columnNameBytes = new byte[columnNameLength];
            file.read(columnNameBytes, 0, columnNameLength);
            String columnName = new String(columnNameBytes, "UTF-16").trim();
            Column column = table.getColumn(columnName);

            readNodePage(rootID, table, treeOrder, loadEngine, file, column.getType(), false);

            BTreeDB bTree = new BTreeDB(table, loadEngine, column.getType(), rootID);

            bTree.setHeight(height);
            bTree.setN(N);
            bTree.setCounter(counter);

            TreeIndex result = new TreeIndex(loadEngine, table, column, bTree);
            result.fillLeafNodes(bTree.getRoot());
            result.setFilled(true);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public final static int INDEX_HEADER_SIZE = Utils.getIntByteSize() * 7 + Type.MAX_STRING_BYTE_SIZE;

    /*
        BTreeIndex layout:
        int : indexType
        int : order of BTree
        int : root id
        int : height
        int : N
        int : counter (amount of nodes)
        int : keyColumn name length
        char[length] : keyColumn name
        ---- for amount of nodes ----
        Node
        ----          end          ----
    */
    public static void serialize(TreeIndex treeIndex, LoadEngine loadEngine, String fileName) {
        BTreeDB bTree = treeIndex.bTree;
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "rw");

            if (file.length() < INDEX_HEADER_SIZE + bTree.getNodeCount() * Page.PAGE_SIZE) {
                file.setLength(INDEX_HEADER_SIZE + bTree.getNodeCount() * Page.PAGE_SIZE);
            }

            file.writeInt(treeIndex.getIndexType().getNum());
            file.writeInt(bTree.getOrder());
            file.writeInt(bTree.getRoot().getID());
            file.writeInt(bTree.getHeight());
            file.writeInt(bTree.getSize());
            file.writeInt(bTree.getNodeCount());

            file.writeInt(treeIndex.column.getName().length() * Utils.getCharByteSize());
            file.writeChars(treeIndex.column.getName());

            //writeNodePage(bTree.getRoot(), loadEngine, file, treeIndex.column.getType());

            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public final static int NODE_SIZE = Utils.getIntByteSize() * 2;

    /*
        Node page layout:
        int : id
        int : currLen (amount of child entries)
        ---- for amount of children ----
        Entry
        ----          end          ----
    */
    public static void writeNodePage(Node node, LoadEngine loadEngine, RandomAccessFile file, Type keyType)
            throws IOException {
        int nodePos = INDEX_HEADER_SIZE + node.getID() * Page.PAGE_SIZE;
        if (file.length() < nodePos + Page.PAGE_SIZE) {
            file.setLength(nodePos + Page.PAGE_SIZE);
        }
        file.seek(nodePos);
        file.writeInt(node.getID());
        file.writeInt(node.currLen);

        for (int i = 0; i < node.currLen; i++) {
            Entry entry = node.children[i];
            file.seek(nodePos + Utils.getIntByteSize() * 2 + i * ENTRY_SIZE);
            writeEntry(entry, file, keyType);
//            if (entry.nextID != -1)
//                writeNodePage((Node) loadEngine.getTreeIndexPageFromBuffer(entry.nextID, node.order, keyType), loadEngine, file, keyType);
        }
    }

    public static void writeNodePage(Page nodePage, LoadEngine loadEngine, RandomAccessFile file, Type keyType)
            throws IOException {
        writeNodePage((Node) nodePage, loadEngine, file, keyType);
    }

    public static int readNodePage(int id, Table table, int order, LoadEngine loadEngine, RandomAccessFile file, Type keyType, boolean onePage)
            throws IOException {
        int nodePos = INDEX_HEADER_SIZE + id * Page.PAGE_SIZE;
        file.seek(nodePos);
        int nodeID = file.readInt();
        int nodeCurrLen = file.readInt();
        Node result = new Node(table, nodeID, nodeCurrLen, order);
        result.dirty = false;

        for (int i = 0; i < nodeCurrLen; i++) {
            file.seek(nodePos + Utils.getIntByteSize() * 2 + i * ENTRY_SIZE);
            result.children[i] = readEntry(table, order, loadEngine, file, keyType, onePage);
        }
        return loadEngine.loadIndexPageInBuffer(result, order, keyType);
    }

    public final static int ENTRY_SIZE = Utils.getMaxObjectSize() + Utils.getIntByteSize() * 2 + 1;

    public final static int ENTRY_COUNT = (Page.PAGE_SIZE - NODE_SIZE) / ENTRY_SIZE;

    /*
        Entry layout:
        object : key
        int    : val
        bool   : val is null
        int    : nextID
    */
    private static void writeEntry(Entry entry, RandomAccessFile file, Type keyType) throws IOException {
        Utils.writeObjectToFile(entry.key, keyType, file);
        file.writeInt(entry.val == null ? 0 : (Integer) entry.val);
        file.writeBoolean(entry.val == null);
        int next = entry.nextID;
        file.writeInt(next);
    }

    private static Entry readEntry(Table table, int order, LoadEngine loadEngine, RandomAccessFile file, Type keyType, boolean onePage)
            throws IOException {
        Entry entry = new Entry(null, null, -1);

        entry.key = (Comparable<Object>) Utils.readObjectFromFile(keyType, file);
        entry.val = file.readInt();
        if (file.readBoolean())
            entry.val = null;
        entry.nextID = file.readInt();
        if (entry.nextID > -1 && !onePage)
            readNodePage(entry.nextID, table, order, loadEngine, file, keyType, onePage);
        return entry;
    }
}
