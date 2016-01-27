package commands_runner.cursors;

import commands_runner.indexes.AbstractIndex;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.table_classes.Record;

/**
 * Created by semionn on 28.10.15.
 */
public class IndexCursor implements ICursor {

    AbstractIndex index;
    Conditions conditions;

    public IndexCursor(AbstractIndex index, Conditions conditions) {
        this.index = index;
        this.conditions = conditions;
        index.setIterator(conditions);
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
}
