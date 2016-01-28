package commands_runner.cursors;

import common.ColumnSelect;
import common.FromClause;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.table_classes.Record;
import common.table_classes.Table;

import java.util.List;

/**
 * Created by semionn on 28.10.15.
 */
public class WhereCursor implements ICursor {

    ICursor cursor;
    Conditions filterConditions;
    Record record = null;
    List<ColumnSelect> columnsSelect;

    public WhereCursor(ICursor cursor, Conditions filterConditions)
    {
        this.cursor = cursor;
        columnsSelect = cursor.getMetaInfo();
        this.filterConditions = filterConditions;
//        this.filterConditions = new Conditions();
//        for (Condition condition : filterConditions.getValues()) {
//            for (ColumnSelect columnSelect : columnsSelect) {
//                if (condition.getTable() == columnSelect.getTable() &&
//                        condition.getColumn() == columnSelect.getСolumn()) {
//                    this.filterConditions.addValue(condition);
//                    break;
//                }
//            }
//        }
    }

    @Override
    public boolean next() {
        do {
            if (!cursor.next())
                return false;
            record = cursor.getRecord();
        } while (!filterConditions.check(record, columnsSelect));
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
