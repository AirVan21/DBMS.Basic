package commands_runner.cursors;

import common.conditions.Conditions;
import common.table_classes.Record;

/**
 * Created by semionn on 30.10.15.
 */
public class JoinCursor implements ICursor {

    ICursor cursorFirst;
    ICursor cursorSecond;
    Conditions conditionsOn;

    public JoinCursor(ICursor cursorFirst, ICursor cursorSecond, Conditions conditionsOn)
    {
        this.conditionsOn = conditionsOn;
        this.cursorFirst = cursorFirst;
        this.cursorSecond = cursorSecond;
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public Record getRecord() {
        return null;
    }
}
