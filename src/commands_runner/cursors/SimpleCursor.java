package commands_runner.cursors;

import buffer_manager.LoadEngine;
import common.Column;
import common.ColumnSelect;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.ArrayList;
import java.util.List;

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
    List<ColumnSelect> columnsSelect;

    int maxRecordsCount;

    public SimpleCursor(LoadEngine loadEngine, Table table){
        this.table = table;
        this.loadEngine = loadEngine;
        maxRecordsCount = Page.calcMaxRecordCount(table.getRecordSize());
        columnsSelect = new ArrayList<>();
        for (Column column : table.getColumns())
            columnsSelect.add(new ColumnSelect(table, column));
        reset();
    }

    @Override
    public boolean next() {
        do {
            recordNum += 1;
            if (recordNum >= maxRecordsCount) {
                recordNum = 0;
                if (loadEngine.sizeInPages() <= pageNum && recordNum >= currentPage.getRecordsCount())
                    return false;
                pageNum += 1;
                loadEngine.switchToTable(table);
                currentPage = loadEngine.getPageFromBuffer(pageNum);
            }
            if (currentPage == null)
                return false;
            currentRecord = currentPage.getRecord(recordNum);
        } while (currentPage.deletedMask.get(recordNum));
        return currentRecord != null;
    }

    @Override
    public Record getRecord() {
        return currentRecord;
    }

    public int getPageNum() {
        return pageNum;
    }

    public int getRecordNum() {
        return recordNum;
    }

    @Override
    public void reset() {
        pageNum = 1;
        recordNum = -1;
        loadEngine.switchToTable(table);
        currentPage = loadEngine.getPageFromBuffer(pageNum);
        currentRecord = null;
    }

    @Override
    public List<ColumnSelect> getMetaInfo() {
        return columnsSelect;
    }
}
