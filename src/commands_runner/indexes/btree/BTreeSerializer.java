package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import common.BaseType;
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
        BTreeDB bTree = new BTreeDB(table);
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "rw");

            int indexType = file.readInt();
            if (indexType != IndexType.BTREE.getNum())
                throw new IllegalArgumentException();

            int treeOrder = file.readInt();
            if (treeOrder != bTree.getOrder())
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

            Node root = readNodePage(rootID, bTree, file, column.getType());

            bTree = new BTreeDB(table, root);

            bTree.setHeight(height);
            bTree.setN(N);
            bTree.setCounter(counter);

            return new TreeIndex(loadEngine, table, column, bTree);
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
    public static void serialize(TreeIndex treeIndex, String fileName) {
        BTreeDB bTree = treeIndex.bTree;
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "rw");

            if (file.length() < INDEX_HEADER_SIZE + bTree.getNodeCount() * Page.PAGE_SIZE) {
                file.setLength(INDEX_HEADER_SIZE + bTree.getNodeCount() * Page.PAGE_SIZE);
            }

            file.writeInt(treeIndex.getIndexType().getNum());
            file.writeInt(bTree.getOrder());
            file.writeInt(bTree.getRoot().id);
            file.writeInt(bTree.getHeight());
            file.writeInt(bTree.getSize());
            file.writeInt(bTree.getNodeCount());

            file.writeInt(treeIndex.column.getName().length() * Utils.getCharByteSize());
            file.writeChars(treeIndex.column.getName());

            writeNodePage(bTree.getRoot(), file, treeIndex.column.getType());

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
    private static void writeNodePage(Node node, RandomAccessFile file, Type keyType) throws IOException {
        int nodePos = INDEX_HEADER_SIZE + node.id * Page.PAGE_SIZE;
        file.seek(nodePos);
        file.writeInt(node.id);
        file.writeInt(node.currLen);

        for (int i = 0; i < node.currLen; i++) {
            Entry entry = node.children[i];
            file.seek(nodePos + Utils.getIntByteSize() * 2 + i * ENTRY_SIZE);
            writeEntry(entry, file, keyType);
            if (entry.next != null)
                writeNodePage(entry.next, file, keyType);
        }
    }

    private static Node readNodePage(int id, BTreeDB btreeDB, RandomAccessFile file, Type keyType) throws IOException {
        int nodePos = INDEX_HEADER_SIZE + id * Page.PAGE_SIZE;
        file.seek(nodePos);
        int nodeID = file.readInt();
        int nodeCurrLen = file.readInt();
        Node result = new Node(btreeDB.getTable(), nodeID, nodeCurrLen, btreeDB.getOrder());

        for (int i = 0; i < nodeCurrLen; i++) {
            file.seek(nodePos + Utils.getIntByteSize() * 2 + i * ENTRY_SIZE);
            result.children[i] = readEntry(btreeDB, file, keyType);
        }
        return result;
    }

    public final static int ENTRY_SIZE = Utils.getMaxObjectSize() + Utils.getIntByteSize() * 2 + 1;

    public final static int ENTRY_COUNT = (Page.PAGE_SIZE - NODE_SIZE) / ENTRY_SIZE;

    /*
        Entry layout:
        object : key
        int    : val
        bool   : val is null
        int    : next
    */
    private static void writeEntry(Entry entry, RandomAccessFile file, Type keyType) throws IOException {
        Utils.writeObjectToFile(entry.key, keyType, file);
        file.writeInt(entry.val == null ? 0 : (Integer) entry.val);
        file.writeBoolean(entry.val == null);
        int next = -1;
        if (entry.next != null)
            next = entry.next.id;
        file.writeInt(next);
    }

    private static Entry readEntry(BTreeDB btreeDB, RandomAccessFile file, Type keyType) throws IOException {
        Entry entry = new Entry(null, null, null);

        entry.key = (Comparable<Object>) Utils.readObjectFromFile(keyType, file);
        entry.val = file.readInt();
        if (file.readBoolean())
            entry.val = null;
        int nextID = file.readInt();
        if (nextID > -1)
            entry.next = readNodePage(nextID, btreeDB, file, keyType);
        return entry;
    }
}
