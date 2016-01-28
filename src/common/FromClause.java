package common;

import common.table_classes.Table;
import org.antlr.v4.runtime.misc.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 28.01.16.
 */
public class FromClause {
    FromClause fristFrom, secondFrom;
    List<Pair<Integer, Integer>> conditionsOn;
    Table table;
    List<Column> columns;

    public FromClause(Table table, List<Column> columns) {
        this.table = table;
        this.columns = columns;
        if (columns == null)
            this.columns = new ArrayList<>();
        conditionsOn = new ArrayList<>();
    }

    public FromClause(FromClause fromFirst, FromClause fromSecond) {
        this.fristFrom = fromFirst;
        this.secondFrom = fromSecond;
        conditionsOn = new ArrayList<>();
    }

    public FromClause getFirstFrom() {
        return fristFrom;
    }

    public FromClause getSecondFrom() {
        return secondFrom;
    }

    public Table getTable() {
        return table;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Pair<Integer, Integer>> getConditionsOn() {
        return conditionsOn;
    }

    public boolean addTableJoinCondition(String firstTableName, String secondTableName, String firstColumnName, String secondColumnName) {
        if (table != null)
            return false;
        firstTableName = firstTableName.toUpperCase();
        secondTableName = secondTableName.toUpperCase();
        if (fristFrom.getTableName().equals(firstTableName) && secondFrom.getTableName().equals(secondTableName)) {
            Integer firstColumnIdx = fristFrom.getTable().getColumnIndex(firstColumnName);
            Integer secondColumnIdx = secondFrom.getTable().getColumnIndex(secondColumnName);
            conditionsOn.add(new Pair<>(firstColumnIdx, secondColumnIdx));
            return true;
        } else if (fristFrom.getTableName().equals(secondTableName) && secondFrom.getTableName().equals(firstTableName)) {
            Integer firstColumnIdx = fristFrom.getTable().getColumnIndex(secondColumnName);
            Integer secondColumnIdx = secondFrom.getTable().getColumnIndex(firstColumnName);
            conditionsOn.add(new Pair<>(firstColumnIdx, secondColumnIdx));
            return true;
        }
        return false;
    }

    public String getTableName() {
        if (table != null)
            return table.getName().toUpperCase();
        return "";
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public Pair<Table, Column> getTableColumn(String columnName) {
        if (table != null)
        {
            Column column = table.getColumn(columnName);
            if (column != null)
                return new Pair<>(table, column);
            return null;
        } else {
            Pair<Table, Column> firstFound = fristFrom.getTableColumn(columnName);
            Pair<Table, Column> secondFound = secondFrom.getTableColumn(columnName);
            if (firstFound != null && secondFound != null && firstFound.a != secondFound.a)
                return null;
            if (firstFound != null)
                return firstFound;
            if (secondFound != null)
                return secondFound;
            return null;
        }
    }
}
