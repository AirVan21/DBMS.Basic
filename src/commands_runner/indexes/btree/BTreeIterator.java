package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import common.conditions.ComparisonType;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.table_classes.Record;
import org.antlr.v4.runtime.misc.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 31.12.15.
 */
public class BTreeIterator {
    BTree bTree;
    List<Node> leafNodes;
    Node currentNode;
    int nodePos;
    int lastNodePos;
    Entry currentEntry;
    int entryPos;
    int lastEntryPos;
    Record currentRecord;
    LoadEngine loadEngine;

    Comparable<Object> leftBound, rightBound;
    boolean isLeftBoundSet, isRightBoundSet;

    public BTreeIterator(TreeIndex treeIndex, BTree bTree, LoadEngine loadEngine, Conditions conditions) {
        this.bTree = bTree;
        this.loadEngine = loadEngine;
        for (Condition condition : conditions.getValues()) {
            if (condition.getColumn() == treeIndex.getColumn()) {
                ComparisonType comparison = condition.getComparisonType();
                if (comparison == ComparisonType.EQUAL) {
                    leftBound = rightBound = condition.getValue();
                    isLeftBoundSet = isRightBoundSet = true;
                } else if (comparison == ComparisonType.GREAT ||
                        comparison == ComparisonType.GREATEQ) {
                    leftBound = condition.getValue();
                    isLeftBoundSet = true;
                } else {
                    rightBound = condition.getValue();
                    isRightBoundSet = true;
                }
            }
        }

        leafNodes = new ArrayList<>();
        fillLeafNodes(bTree.getRoot());
        currentRecord = null;
        entryPos = -1;
        nodePos = 0;
        currentNode = leafNodes.get(0);
        if (isLeftBoundSet) {
            Pair<Node, Integer> pair = bTree.getPosition(leftBound);
            currentNode = pair.a;
            entryPos = pair.b - 1;
            nodePos = leafNodes.indexOf(currentNode);
        }
        if (isRightBoundSet) {
            Pair<Node, Integer> pair = bTree.getPosition(rightBound);
            lastNodePos = leafNodes.indexOf(pair.a);
            lastEntryPos = pair.b;
        }
    }

    private void fillLeafNodes(Node node) {
        if (node.currLen > 0)
            if (node.children[0].next == null)
                leafNodes.add(node);
            else
                for (int i = 0; i < node.currLen; i++) {
                    Entry entry = node.children[i];
                    if (entry != null)
                        fillLeafNodes(entry.next);
                }
    }

    public boolean next() {
        if (isRightBoundSet && nodePos >= lastNodePos && entryPos > lastEntryPos)
            return false;
        entryPos++;
        if (entryPos >= currentNode.currLen) {
            nodePos++;
            if (nodePos >= leafNodes.size())
                return false;
            currentNode = leafNodes.get(nodePos);
            entryPos = 0;
        }
        currentEntry = currentNode.children[entryPos];
        loadEngine.switchToTable(bTree.getTable());
        currentRecord = loadEngine.getRecordByOffset((Integer) currentEntry.val);
        return true;
    }

    public Record getRecord() {
        return currentRecord;
    }
}
