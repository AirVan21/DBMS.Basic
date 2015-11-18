package common.table_classes;

import common.conditions.Conditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class Page {
    int pageId; //?
    int pinCount;
    boolean deleted;
    boolean dirty;
    boolean full;

    // KBytes
    public static final int PAGE_SIZE = 4 * 1024;

    static final int HEADER_SIZE = 8 + 3; //TODO: more accurate and explicit

    Table table;
    ArrayList<Record> records;
    int loadedRecordsCount;

    Page(Table table) {
        this.table = table;
        loadedRecordsCount = 0;
        records = new ArrayList<>();
    }

    public Table getTable() {
        return table;
    }

    void readHeader() {
        //TODO: read header from file and fill params
        pinCount = 0;
        deleted = false;
        dirty = false;
        full = false;
    }

    public Record getRecord(int num) {
        if (num >= loadedRecordsCount)
            for (int i = loadedRecordsCount - 1; i < num; i++) {
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

}
