package commands_runner.cursors;

import common.Column;
import common.conditions.Conditions;
import common.table_classes.Record;
import org.antlr.v4.runtime.misc.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 30.10.15.
 */
public class JoinCursor implements ICursor {

    ICursor cursorFirst;
    ICursor cursorSecond;
    List<Pair<Integer, Integer>> conditionsOn;
    Record currentRecord;

    public JoinCursor(ICursor cursorFirst, ICursor cursorSecond, List<Pair<Integer, Integer>> conditionsOn)
    {
        this.conditionsOn = conditionsOn;
        this.cursorFirst = cursorFirst;
        this.cursorSecond = cursorSecond;
        this.currentRecord = null;

    }

    @Override
    public boolean next() {
        while (cursorFirst.next()) {
            Record firstRec = cursorFirst.getRecord();
            while (cursorSecond.next()) {
                Record secondRec = cursorSecond.getRecord();
                for (Pair<Integer, Integer> columnIndexes : conditionsOn)
                    if (firstRec.getColumnValue(columnIndexes.a).equals(secondRec.getColumnValue(columnIndexes.b))) {
                        List<Object> values = new ArrayList<>(firstRec.values);
                        values.addAll(secondRec.values);
                        currentRecord = new Record(values);
                        return true;
                    }
            }
            cursorSecond.reset();
        }
        return false;
    }

    @Override
    public Record getRecord() {
        return currentRecord;
    }

    @Override
    public void reset() {
        cursorFirst.reset();
        cursorSecond.reset();
    }
}
