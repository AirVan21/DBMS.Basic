package commands_runner.indexes.btree;

import common.table_classes.Page;
import common.table_classes.Table;
import org.antlr.v4.runtime.misc.Pair;

/**
 * Created by semionn on 01.01.16.
 */

class Node extends Page {
    int currLen;
    int order;
    int id;
    Entry[] children;

    public Node(Table table, int id, int m, int order) {
        super(table);
        this.id = id;
        this.currLen = m;
        this.order = order;
        this.children = new Entry[order];
    }
}

class Entry {
    Comparable key;
    Object val;
    Node next;

    public Entry(Comparable key, Object val, Node next) {
        this.key = key;
        this.val = val;
        this.next = next;
    }
}

class BTreeDB extends BTree<Comparable<Object>, Integer> {
    public BTreeDB(Table table) {
        super(table);
    }
}

class BTree<Key extends Comparable<Object>, Value> {

    private Node root;
    private int height;
    private int N;
    private Table table;
    private int order;
    private int counter;

    public BTree(Table table) {
        this.order = table.calcMaxRecordCount();
        if (order % 2 != 0)
            order++;
        counter = 0;
        root = new Node(table, incCounter(), 0, order);
        this.table = table;
    }

    private int incCounter() {
        return counter++;
    }

    public Node getRoot() {
        return root;
    }

    public boolean isEmpty() {
        return getSize() == 0;
    }

    public int getSize() {
        return N;
    }

    public int getHeight() {
        return height;
    }

    public Table getTable() {
        return table;
    }

    public Value get(Key key) {
        if (key == null)
            throw new NullPointerException("key must not be null");
        return search(root, key, height);
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
                    return search(children[j].next, key, ht - 1);
            }
        }
        return null;
    }

    public Pair<Node, Integer> getPosition(Key key) {
        if (key == null)
            throw new NullPointerException("key must not be null");
        return searchPosition(root, key, height);
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
        return searchPosition(children[leftEntryNum].next, key, ht - 1);
    }

    public void put(Key key, Value val) {
        if (key == null) throw new NullPointerException("key must not be null");
        Node u = insert(root, key, val, height);
        N++;
        if (u == null) return;

        Node t = new Node(table, incCounter(), 2, order);
        t.children[0] = new Entry(root.children[0].key, null, root);
        t.children[1] = new Entry(u.children[0].key, null, u);
        root = t;
        height++;
    }

    private Node insert(Node h, Key key, Value val, int ht) {
        int j;
        Entry t = new Entry(key, val, null);

        if (ht == 0) {
            for (j = 0; j < h.currLen; j++) {
                if (less(key, h.children[j].key)) break;
            }
        } else {
            for (j = 0; j < h.currLen; j++) {
                if ((j + 1 == h.currLen) || less(key, h.children[j + 1].key)) {
                    Node u = insert(h.children[j++].next, key, val, ht - 1);
                    if (u == null) return null;
                    t.key = u.children[0].key;
                    t.next = u;
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
        Node t = new Node(table, incCounter(), order / 2, order);
        h.currLen = order / 2;
        for (int j = 0; j < order / 2; j++)
            t.children[j] = h.children[order / 2 + j];
        return t;
    }

    public String toString() {
        return toString(root, height, "") + "\n";
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
                s.append(toString(children[j].next, ht - 1, indent + "     "));
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

