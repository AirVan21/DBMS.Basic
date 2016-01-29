package common.table_classes;

import commands_runner.cursors.IndexCursor;
import commands_runner.indexes.btree.IndexType;
import common.Column;
import common.Type;
import common.conditions.Conditions;
import common.utils.Utils;
import org.antlr.v4.runtime.misc.Pair;

import javax.naming.OperationNotSupportedException;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class Page {
    public int pageId;
//    public int pinCount;
    public boolean deleted;
    public boolean dirty;
    public boolean full;
    public Table table;
    public BitSet deletedMask;

    // KBytes
    public static final int PAGE_SIZE = 4 * 1024;

    public static final int HEADER_SIZE = Utils.getIntByteSize() * 3 + 3; //pageID + recordCount + deleteMaskSize + deleted(1) + dirty(1) + full(1)

    ArrayList<Record> records;

    final int maxRecordCount;

    public Page(Table table) {
        pageId = 1;
        this.table = table;
        maxRecordCount = calcMaxRecordCount(table.recordSize);
        records = new ArrayList<>();
        deletedMask = new BitSet(maxRecordCount);
    }

    public static int calcMaxRecordCount(int recordSize) {
        final int bitsInByte = 8;
        int pureRecordCount = (PAGE_SIZE - HEADER_SIZE) / recordSize;
        int deleteMaskSize = (pureRecordCount / (Utils.getLongByteSize() * bitsInByte) + 1) * 3 * Utils.getLongByteSize() * bitsInByte; // BitSet is a long array[]
        int resultRecordCount = (PAGE_SIZE - HEADER_SIZE - deleteMaskSize) / recordSize;
        return resultRecordCount;
    }

    public Record getRecord(int num) {
        if (num >= records.size())
            return null;
        return records.get(num);
    }

    public List<Record> getAllRecords() {
        return records;
    }

    public boolean isFull() {
        return full;
    }

    public void addRecord(Record record) {
        records.add(record);
        dirty = true;
        if (records.size() >= maxRecordCount)
            full = true;
    }

    public void refreshPage() {
        records.clear();
    }

    public int getRecordsCount() {
        return records.size();
    }

    public boolean isIndex() {
        return false;
    }

    public IndexType getIndexType() throws OperationNotSupportedException {
        throw new OperationNotSupportedException();
    }

    public int deleteRecords(Conditions conditions) {
        int removedCount = 0;
        for (int i = 0; i < records.size(); i++) {
            if (conditions.check(records.get(i), null)) {
                deletedMask.set(i);
                removedCount++;
            }
        }
        return removedCount;
    }

    public int updateRecords(Conditions conditions, List<Pair<Integer, Object>> assignments) {
        int updatedCount = 0;
        for (int i = 0; i < records.size(); i++) {
            if (conditions.check(records.get(i), null)) {
                records.get(i).update(assignments);
                updatedCount++;
            }
        }
        return updatedCount;

    }


}
