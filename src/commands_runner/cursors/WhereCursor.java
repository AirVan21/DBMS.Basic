package commands_runner.cursors;

import common.ColumnSelect;
import common.conditions.Conditions;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.List;

/**
 * Created by semionn on 28.10.15.
 */
public class WhereCursor implements ICursor {

    Table table;
    ICursor cursor;
    Conditions filterConditions;
    Record record = null;
    List<ColumnSelect> columnsSelect;

    public WhereCursor(ICursor cursor, Conditions filterConditions, Table table)
    {
        this.filterConditions = filterConditions;
        this.cursor = cursor;
        this.table = table;
        columnsSelect = cursor.getMetaInfo();
    }

    @Override
    public boolean next() {
        do {
            if (!cursor.next())
                return false;
            record = cursor.getRecord();
        } while (!filterConditions.check(record));
        return true;
    }

    @Override
    public Record getRecord() {
        return cursor.getRecord();
    }

    @Override
    public void reset() {
        cursor.reset();
    }

    @Override
    public List<ColumnSelect> getMetaInfo() {
        return columnsSelect;
    }
}
