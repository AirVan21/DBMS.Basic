package commands_runner.indexes;

import commands_runner.indexes.btree.IndexType;
import common.Type;
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

    @Override
    public IndexType getIndexType() {
        return IndexType.HASH;
    }

    @Override
    public Type getKeyType() {
        throw new NotImplementedException();
    }

    @Override
    public void fillIndex() {
        throw new NotImplementedException();
    }
}
