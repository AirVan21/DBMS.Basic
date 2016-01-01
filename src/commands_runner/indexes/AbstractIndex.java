package commands_runner.indexes;

import commands_runner.indexes.btree.IndexType;
import common.conditions.Conditions;
import common.table_classes.Record;

/**
 * Created by semionn on 30.10.15.
 */
public abstract class AbstractIndex {
    public abstract IndexType getIndexType();
    public abstract boolean next();
    public abstract Record getRecord();
    public abstract void setIterator(Conditions conditions);
}
