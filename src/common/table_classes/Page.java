package common.table_classes;

import commands_runner.cursors.IndexCursor;
import commands_runner.indexes.btree.IndexType;
import common.Column;
import common.Type;
import common.conditions.Conditions;
import common.utils.Utils;

import javax.naming.OperationNotSupportedException;
import java.io.UnsupportedEncodingException;
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
    public ByteBuffer pageBuffer;

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
        pageBuffer = ByteBuffer.allocate(PAGE_SIZE);
        initRecordPageBuffer();
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
        addRecordToPageBuffer(record);
        dirty = true;
        if (records.size() >= maxRecordCount)
            full = true;
        updateRecordPageBuffer();
    }

    public void updateRecordPageBuffer() {
        // Updating default meta-information
        final int INT_SIZE = 4;
        pageBuffer.putInt(0, pageId);
        pageBuffer.put(INT_SIZE    , (byte) (deleted ? 1 : 0));
        pageBuffer.put(INT_SIZE + 1, (byte) (dirty   ? 1 : 0));
        pageBuffer.put(INT_SIZE + 2, (byte) (full    ? 1 : 0));
        pageBuffer.putInt(INT_SIZE + 3, records.size());
    }

    public void refreshPage() {
        records.clear();
        pageBuffer.clear();
        initRecordPageBuffer();
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

    private void addRecordToPageBuffer(Record record) {
        List<Column> columns = table.getColumns();
        for (int i = 0; i < table.getColumns().size(); i++) {
            Object value = record.getColumnValue(i);
            pageBuffer.putInt(columns.get(i).getType().getBaseType().getTypeNumber());
            switch (columns.get(i).getType().getBaseType()) {
                case VARCHAR:
                    ByteBuffer stringBuffer = ByteBuffer.allocate(Type.MAX_STRING_BYTE_SIZE);
                    try {
                        stringBuffer.put(((String) value).getBytes("UTF-16"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    pageBuffer.put(stringBuffer.array());
                    break;
                case DOUBLE:
                    pageBuffer.putDouble((double) value);
                    break;
                case INT:
                    pageBuffer.putInt((int) value);
                    break;
            }
        }
    }

    private void initRecordPageBuffer() {
        // Setting default meta-information
        pageBuffer.putInt(pageId);
        pageBuffer.put((byte) (deleted ? 1 : 0));
        pageBuffer.put((byte) (dirty   ? 1 : 0));
        pageBuffer.put((byte) (full    ? 1 : 0));
        pageBuffer.putInt(records.size());
    }

}
