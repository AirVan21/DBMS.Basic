package common;

import common.conditions.Conditions;
import common.table_classes.Table;

/**
 * Created by semionn on 28.10.15.
 */
public class Join {
    Table tableA;
    Table tableB;
    Conditions conditionsOn;

    Join(Table tableA, Table tableB, Conditions conditionsOn) {
        this.conditionsOn = conditionsOn;
        this.tableA = tableA;
        this.tableB = tableB;
    }

    public Table getTableA() {
        return tableA;
    }

    public Table getTableB() {
        return tableB;
    }

    public Conditions getConditionsOn() {
        return conditionsOn;
    }
}
