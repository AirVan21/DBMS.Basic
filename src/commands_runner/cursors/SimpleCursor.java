package commands_runner.cursors;

import buffer_manager.LoadEngine;
import common.exceptions.ReadPageException;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;

/**
 * Created by semionn on 28.10.15.
 */
public class SimpleCursor implements ICursor {

    Table table;
    LoadEngine loadEngine;
    int pageNum;
    int recordNum;
    Record currentRecord;
    Page currentPage;

    int maxRecordsCount;

    public SimpleCursor(LoadEngine loadEngine, Table table){
        this.table = table;
        this.loadEngine = loadEngine;
        pageNum = 1;
        recordNum = -1;
        maxRecordsCount = Page.calcMaxRecordCount(table.getRecordSize());
        loadEngine.switchToTable(table);
        try {
            currentPage = loadEngine.getPageFromBuffer(pageNum);
        } catch (ReadPageException e) {
            currentPage = null;
        }
        currentRecord = null;
    }

    @Override
    public boolean next() {
        recordNum += 1;
        if (recordNum >= maxRecordsCount) {
            recordNum = 0;
            if (loadEngine.sizeInPages() <= pageNum && recordNum >= currentPage.getRecordsCount())
                return false;
            pageNum += 1;
            loadEngine.switchToTable(table);
            try {
                currentPage = loadEngine.getPageFromBuffer(pageNum);
            } catch (ReadPageException e) {
                currentPage = null;
            }
        }
        if (currentPage == null)
            return false;
        currentRecord = currentPage.getRecord(recordNum);
        if (currentRecord == null)
            return false;
        return true;
    }

    @Override
    public Record getRecord() {
        return currentRecord;
    }
}
