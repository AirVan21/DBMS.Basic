package commands_runner.indexes;

import common.conditions.Conditions;
import common.table_classes.Record;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by semionn on 30.10.15.
 */
public class HashIndex extends AbstractIndex {
    @Override
    public boolean next() {
        throw new NotImplementedException();
    }

    @Override
    public Record getRecord() {
        throw new NotImplementedException();
    }

    @Override
    public void setIterator(Conditions conditions) {

    }
}
