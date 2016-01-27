package commands_runner.cursors;

import common.ColumnSelect;
import common.table_classes.Record;

import java.util.List;

/**
 * Created by semionn on 28.10.15.
 */
public interface ICursor {
    boolean next();
    Record getRecord();
    void reset();
    List<ColumnSelect> getMetaInfo();
}
