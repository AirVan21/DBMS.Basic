package commands_runner.cursors;

import common.table_classes.Record;

/**
 * Created by semionn on 28.10.15.
 */
public interface ICursor {
    boolean next();
    Record getRecord();
}
