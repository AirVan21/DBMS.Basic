package commands_runner.cursors;

import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.List;

/**
 * Created by semionn on 28.10.15.
 */
public class SimpleCursor implements ICursor {

    Table table;
    List<Page> pages;
    int pageNum;
    int recordNum;
    Record currentRecord;

    int recordsCount;

    public SimpleCursor(List<Page> pages, Table table){
        this.table = table;
        this.pages = pages;
        pageNum = 0;
        recordNum = 0;
        recordsCount = Page.PAGE_SIZE / table.getRecordSize();
        currentRecord = pages.get(pageNum).getRecord(recordNum);
    }

    @Override
    public boolean next() {
        recordNum += 1;
        if (recordNum >= recordsCount) {
            recordNum = 0;
            pageNum += 1;
            if (pageNum >= pages.size())
                return false;
        }
        currentRecord = pages.get(pageNum).getRecord(recordNum);
        return true;
    }

    @Override
    public Record getRecord() {
        return currentRecord;
    }
}
