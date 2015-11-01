package commands_runner.cursors;

import common.table_classes.Record;

/**
 * Created by semionn on 28.10.15.
 */
public class IndexCursor implements ICursor {
    @Override
    public boolean next() {
        return false;
    }

    @Override
    public Record getRecord() {
        return null;
    }
}
