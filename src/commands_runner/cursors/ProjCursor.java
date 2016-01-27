package commands_runner.cursors;

import common.Column;
import common.ColumnSelect;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 27.01.16.
 */
public class ProjCursor implements ICursor {

    ICursor baseCursor;
    List<Integer> columnIndexes;
    List<ColumnSelect> selectionColumns;
    Record currentRecord;

    public ProjCursor(ICursor baseCursor, List<ColumnSelect> selectionColumns) {
        this.baseCursor = baseCursor;
        this.columnIndexes = new ArrayList<>();
        this.selectionColumns = selectionColumns;
        currentRecord = null;
        for (ColumnSelect columnSelect : selectionColumns) {
            columnIndexes.add(columnSelect.getTable().getColumnIndex(columnSelect.getСolumn()));
        }
    }

    @Override
    public boolean next() {
        if (baseCursor.next()) {
            currentRecord = new Record(baseCursor.getRecord().getColumnsValues(columnIndexes));
            return true;
        }
        return false;
    }

    @Override
    public Record getRecord() {
        return currentRecord;
    }

    @Override
    public void reset() {
        baseCursor.reset();
    }

    @Override
    public List<ColumnSelect> getMetaInfo() {
        return selectionColumns;
    }
}
