package common.table_classes;

import common.conditions.Conditions;

import java.util.ArrayList;
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

    // KBytes
    public static final int PAGE_SIZE = 4 * 1024;

    public static final int HEADER_SIZE = 4 + 3 + 4; //pageID(4) + deleted(1) + dirty(1) + full(1) + recordCount(4)

    ArrayList<Record> records;

    final int maxRecordCount;

    public Page(Table table) {
        pageId = 1;
        this.table = table;
        maxRecordCount = calcMaxRecordCount(table.recordSize);
        records = new ArrayList<>();
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

}
