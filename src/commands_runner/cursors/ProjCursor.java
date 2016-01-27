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
    Record currentRecord;

    ProjCursor(ICursor baseCursor, List<ColumnSelect> selectionColumns) {
        this.baseCursor = baseCursor;
        this.columnIndexes = new ArrayList<>();
        currentRecord = null;
        for (ColumnSelect column : selectionColumns) {
            //columnIndexes.add(table.getColumnIndex(column));
        }
    }

    @Override
    public boolean next() {
        while (baseCursor.next()) {
//            currentRecord = new Record(columnIndexes);
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
}
