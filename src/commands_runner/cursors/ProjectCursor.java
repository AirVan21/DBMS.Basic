package commands_runner.cursors;

import common.conditions.Conditions;
import common.table_classes.Record;
import common.table_classes.Table;

/**
 * Created by semionn on 28.10.15.
 */
public class ProjectCursor implements ICursor {

    Table table;

    Conditions filterConditions;

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public Record getRecord() {
        return null;
    }
}
