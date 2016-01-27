package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import common.Type;
import common.table_classes.Page;
import common.table_classes.Table;
import org.antlr.v4.runtime.misc.Pair;

/**
 * Created by semionn on 01.01.16.
 */

class Node extends Page {
    int currLen;
    int order;
    Entry[] children;

    public Node(Table table, int id, int currLen, int order) {
        super(table);
        this.pageId = id;
        this.currLen = currLen;
        this.order = order;
        this.children = new Entry[order];
        this.dirty = true;
    }

    public void setID(int id) {
        pageId = id;
    }

    public int getID() {
        return pageId;
    }

    @Override
    public boolean isIndex() {
        return true;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.BTREE;
    }
}

class Entry {
    public Comparable key;
    public Object val;
    public int nextID;

    public Entry(Comparable key, Object val, int nextID) {
        this.key = key;
        this.val = val;
        this.nextID = nextID;
    }
}

class BTreeDB extends BTree<Comparable<Object>, Integer> {
    public BTreeDB(Table table, LoadEngine loadEngine, Type keyType) {
        super(table, loadEngine, keyType);
    }

    public BTreeDB(Table table, LoadEngine loadEngine, Type keyType, int rootID) {
        super(table, loadEngine, keyType, rootID);
    }
}

class BTree<Key extends Comparable<Object>, Value> {

    private int rootID;
    private int height;
    private int N;
    private Table table;
    private LoadEngine loadEngine;
    private int order;
    private int counter;
    private Type keyType;

    public BTree(Table table, LoadEngine loadEngine, Type keyType) {
        this(table, loadEngine, keyType, -1);
    }

    public BTree(Table table, LoadEngine loadEngine, Type keyType, int rootID) {
        this.order = BTreeSerializer.ENTRY_COUNT;
        this.keyType = keyType;
        this.table = table;
        this.loadEngine = loadEngine;
        if (order % 2 != 0)
            order++;
        counter = 1;
        if (rootID != -1)
            this.rootID = rootID;
        else {
            Node node = createNode(rootID, order, -1);
            this.rootID = node.getID();
        }
    }

    private Node createNode(int currLen, int order, int id) {
        if (id == -1)
            id = incCounter();
        Node node = new Node(table, id, currLen, order);
        loadEngine.loadIndexPageInBuffer(node, order, keyType);
        return node;
    }

    private int incCounter() {
        return counter++;
    }

    public Node getRoot() {
        return loadNode(rootID);
    }

    public int getSize() {
        return N;
    }

    public int getNodeCount() {
        return counter;
    }

    public int getHeight() {
        return height;
    }

    public int getOrder() {
        return order;
    }

    public Type getKeyType() {
        return keyType;
    }

    public Table getTable() {
        return table;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public void setN(int n) {
        N = n;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public Node loadNode(int nodeID) {
        Page result = loadEngine.getTreeIndexPageFromBuffer(nodeID, order, keyType);
        return (Node) result;
    }
    public Value get(Key key) {
        if (key == null)
            throw new NullPointerException("key must not be null");
        return search(getRoot(), key, height);
    }

    private Value search(Node x, Key key, int ht) {
        Entry[] children = x.children;

        if (ht == 0) {
            for (int j = 0; j < x.currLen; j++) {
                if (eq(key, children[j].key))
                    return (Value) children[j].val;
            }
        } else {
            for (int j = 0; j < x.currLen; j++) {
                if (j + 1 == x.currLen || less(key, children[j + 1].key))
                    return search(loadNode(children[j].nextID), key, ht - 1);
            }
        }
        return null;
    }

    public Pair<Node, Integer> getPosition(Key key) {
        if (key == null)
            throw new NullPointerException("key must not be null");
        return searchPosition(getRoot(), key, height);
    }

    private Pair<Node, Integer> searchPosition(Node x, Key key, int ht) {
        Entry[] children = x.children;

        int leftEntryNum = 0;
        int rightEntryNum = x.currLen - 1;
        while (leftEntryNum < rightEntryNum) {
            int middleEntryNum = (int) Math.ceil((leftEntryNum + rightEntryNum) * 1.0 / 2);
            if (eq(children[middleEntryNum].key, key)) {
                leftEntryNum = middleEntryNum;
                break;
            } else if (less(children[middleEntryNum].key, key)) {
                leftEntryNum = middleEntryNum;
            } else {
                rightEntryNum = middleEntryNum - 1;
            }
        }
        if (ht == 0)
            return new Pair<>(x, leftEntryNum);
        return searchPosition(loadNode(children[leftEntryNum].nextID), key, ht - 1);
    }

    public void put(Key key, Value val) {
        if (key == null) throw new NullPointerException("key must not be null");
        Node u = insert(getRoot(), key, val, height);
        N++;
        if (counter > 16) {
            for (int i = 0; i < 16; i++)
                loadEngine.storeIndexPageInFile(i, order, keyType);
        }
        //save(getRoot(), key, height);

        if (u == null) return;

        Node rootNode = getRoot();
        rootNode.setID(incCounter());

        Node t = createNode(2, order, rootID);
        t.children[0] = new Entry(rootNode.children[0].key, null, rootNode.pageId);
        t.children[1] = new Entry(u.children[0].key, null, u.getID());
        height++;
    }

    private void save(Node h, Key key, int ht) {
        int j;
        if (ht != 0) {
            for (j = 0; j < h.currLen; j++) {
//                    loadEngine.storeIndexPageInFile(h.children[j].nextID, order, keyType);
                if ((j + 1 == h.currLen) || less(key, h.children[j + 1].key)) {
                    save(loadNode(h.children[j++].nextID), key, ht - 1);
                    return;
                }
            }
        }
        //loadEngine.storeIndexPageInFile(h.getID(), order, keyType);
    }

    private Node insert(Node h, Key key, Value val, int ht) {
        h.dirty = true;
        int j;
        Entry t = new Entry(key, val, -1);

        if (ht == 0) {
            for (j = 0; j < h.currLen; j++) {
                if (less(key, h.children[j].key)) break;
            }
        } else {
            for (j = 0; j < h.currLen; j++) {
                if ((j + 1 == h.currLen) || less(key, h.children[j + 1].key)) {
                    Node u = insert(loadNode(h.children[j++].nextID), key, val, ht - 1);
                    if (u == null) return null;
                    t.key = u.children[0].key;
                    t.nextID = u.getID();
                    break;
                }
            }
        }

        for (int i = h.currLen; i > j; i--)
            h.children[i] = h.children[i - 1];
        h.children[j] = t;
        h.currLen++;
        if (h.currLen < order)
            return null;
        else
            return split(h);
    }


    private Node split(Node h) {
        Node t = createNode(order / 2, order, -1);
        h.currLen = order / 2;
        for (int j = 0; j < order / 2; j++)
            t.children[j] = h.children[order / 2 + j];
//        loadEngine.storeIndexPageInFile(t.getID(), order, keyType);
        return t;
    }

    public String toString() {
        return toString(getRoot(), height, "") + "\n";
    }

    private String toString(Node h, int ht, String indent) {
        StringBuilder s = new StringBuilder();
        Entry[] children = h.children;

        if (ht == 0) {
            for (int j = 0; j < h.currLen; j++) {
                s.append(indent + children[j].key + ":" + children[j].val + "\n");
            }
        } else {
            for (int j = 0; j < h.currLen; j++) {
                if (j > 0) s.append(indent + "(" + children[j].key + ")\n");
                s.append(toString(loadNode(children[j].nextID), ht - 1, indent + "     "));
            }
        }
        return s.toString();
    }

    private boolean less(Comparable k1, Comparable k2) {
        return k1.compareTo(k2) < 0;
    }

    private boolean eq(Comparable k1, Comparable k2) {
        return k1.compareTo(k2) == 0;
    }

}

