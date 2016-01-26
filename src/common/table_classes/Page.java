package common.table_classes;

import commands_runner.cursors.IndexCursor;
import commands_runner.indexes.btree.IndexType;
import common.conditions.Conditions;
import common.utils.Utils;

import javax.naming.OperationNotSupportedException;
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

    public static final int HEADER_SIZE = Utils.getIntByteSize() * 2 + 3; //pageID + recordCount + deleted(1) + dirty(1) + full(1)

    ArrayList<Record> records;

    final int maxRecordCount;

    public Page(Table table) {
        pageId = 1;
        this.table = table;
        maxRecordCount = calcMaxRecordCount(table.recordSize);
        records = new ArrayList<>();
        deletedMask = new BitSet();
    }

    public static int calcMaxRecordCount(int recordSize) {
        return (PAGE_SIZE - HEADER_SIZE) / recordSize;
    }

    public Record getRecord(int num) {
        if (num >= records.size())
            return null;
        return records.get(num);
    }

    public List<Record> getRecords(Conditions conditions) {
        List<Record> result = new ArrayList<>();
        for (Record record : records) {
            if (conditions.check(record)) {
                result.add(record);
            }
        }
        return result;
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
            if (conditions.check(records.get(i))) {
                deletedMask.set(i);
                removedCount++;
            }
        }
        return removedCount;
    }
}
