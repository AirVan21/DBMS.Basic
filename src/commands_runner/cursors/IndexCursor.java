package commands_runner.cursors;

import commands_runner.indexes.AbstractIndex;
import common.Column;
import common.ColumnSelect;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 28.10.15.
 */
public class IndexCursor implements ICursor {

    AbstractIndex index;
    Conditions conditions;
    List<ColumnSelect> columnsSelect;

    public IndexCursor(Table table, AbstractIndex index, Conditions conditions) {
        this.index = index;
        this.conditions = conditions;
        index.setIterator(conditions);
        columnsSelect = new ArrayList<>();
        for (Column column : table.getColumns())
            columnsSelect.add(new ColumnSelect(table, column));
    }

    @Override
    public boolean next() {
        return index.next();
    }

    @Override
    public Record getRecord() {
        return index.getRecord();
    }

    @Override
    public void reset() {
        index.setIterator(conditions);
    }

    @Override
    public List<ColumnSelect> getMetaInfo() {
        return columnsSelect;
    }
}
