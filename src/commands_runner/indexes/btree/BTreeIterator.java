package commands_runner.indexes.btree;

import buffer_manager.LoadEngine;
import common.Type;
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
    BTreeDB bTree;
    List<Integer> leafNodes;
    Node currentNode;
    int nodePos;
    int lastNodePos;
    Entry currentEntry;
    int entryPos;
    int lastEntryPos;
    Record currentRecord;
    LoadEngine loadEngine;
    Type keyType;

    Comparable<Object> leftBound, rightBound;
    boolean isLeftBoundSet, isRightBoundSet;

    public BTreeIterator(TreeIndex treeIndex, BTreeDB bTree, LoadEngine loadEngine, Conditions conditions, List<Integer> leafNodes) {
        this.bTree = bTree;
        this.loadEngine = loadEngine;
        this.leafNodes = leafNodes;
        keyType = treeIndex.getColumn().getType();

        setBounds(treeIndex, conditions);
        currentRecord = null;
        entryPos = -1;
        nodePos = 0;
        if (isLeftBoundSet) {
            Pair<Node, Integer> pair = bTree.getPosition(leftBound);
            currentNode = pair.a;
            entryPos = pair.b - 1;
            nodePos = leafNodes.indexOf(currentNode.getID());
        }
        else {
            int nodeID = leafNodes.get(nodePos);
            currentNode = (Node) loadEngine.getTreeIndexPageFromBuffer(nodeID, bTree.getOrder(), bTree.getKeyType());
        }
        if (isRightBoundSet) {
            Pair<Node, Integer> pair = bTree.getPosition(rightBound);
            lastNodePos = leafNodes.indexOf(pair.a.getID());
            lastEntryPos = pair.b;
        }
    }

    private void setBounds(TreeIndex treeIndex, Conditions conditions) {
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
    }
    public boolean next() {
        if (isRightBoundSet && (nodePos == lastNodePos && entryPos > lastEntryPos || nodePos > lastNodePos) )
            return false;
        entryPos++;
        if (entryPos >= currentNode.currLen) {
            nodePos++;
            if (nodePos >= leafNodes.size())
                return false;
            int nodeID = leafNodes.get(nodePos);
            currentNode = (Node) loadEngine.getTreeIndexPageFromBuffer(nodeID, bTree.getOrder(), bTree.getKeyType());
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
