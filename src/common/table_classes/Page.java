package common.table_classes;

import common.conditions.Conditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class Page {
    public int pageId;
    public int pinCount;
    public boolean deleted;
    public boolean dirty;
    public boolean full;
    public Table table;

    // KBytes
    public static final int PAGE_SIZE = 4 * 1024;

    static final int HEADER_SIZE = 8 + 3; //TODO: more accurate and explicit

    ArrayList<Record> records;
    int recordsCount;

    final int maxRecordCount;

    public Page(Table table) {
        recordsCount = 0;
        this.table = table;
        maxRecordCount = (PAGE_SIZE - HEADER_SIZE) / table.recordSize;
        records = new ArrayList<>();
    }

    public Record getRecord(int num) {
        if (num >= recordsCount)
            for (int i = recordsCount - 1; i < num; i++) {
                readRecord(i);
            }
        return records.get(num);
    }

    void readRecord(int num) {
        //TODO: read record from file
        //calc offset as table_header_size + page_size * pagenum + header_size + num * record_size;
        //DataInputStream in = new DataInputStream(new FileInputStream(table.getFileName()));
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
        recordsCount += 1;
        if (recordsCount >= maxRecordCount)
            full = true;
    }

    public int getRecordsCount() {
        return recordsCount;
    }

}
