package parser;

import commands_runner.ITableManager;
import common.*;
import common.conditions.ComparisonType;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Table;
import org.antlr.v4.runtime.misc.Pair;
import parser.sql_antlr.SQLiteBaseListener;
import parser.sql_antlr.SQLiteParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 27.10.15.
 */
public class SQLListener extends SQLiteBaseListener {

    ITableManager tableManager;
    String error_message = "";

    StatementType statementType = StatementType.NONE;
    Map<String,Object> params = new HashMap<>();

    public SQLListener(ITableManager tableManager) {
        this.tableManager = tableManager;
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
    }

    @Override
    public void exitCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        statementType = StatementType.CREATE;
        params.put("db_name", ctx.database_name().getText());
        params.put("table_name", ctx.table_name().getText());

        List<Column> columns = new ArrayList<>();
        List<SQLiteParser.Column_defContext> tempList = ctx.column_def();
        for (SQLiteParser.Column_defContext column_defContext : tempList) {
            String name = column_defContext.column_name().getText();
            String type = column_defContext.type_name().getText();
            columns.add(new Column(name, Type.createType(type)));
        }
        params.put("columns", columns);
    }

    @Override
    public void enterSelect_core(SQLiteParser.Select_coreContext ctx) {

    }

    @Override
    public void exitSelect_core(SQLiteParser.Select_coreContext ctx) {
        statementType = StatementType.SELECT;
        try {
            FromClause fromClause;
            if (ctx.table_or_subquery(0) != null) {
                fromClause = new FromClause(tableManager.getTable(ctx.table_or_subquery(0).table_name().getText()), null);
                params.put("from", fromClause);
            } else {
                Table table1 = tableManager.getTable(ctx.join_clause().table_or_subquery(0).table_name().getText());
                Table table2 = tableManager.getTable(ctx.join_clause().table_or_subquery(1).table_name().getText());
                fromClause = new FromClause(new FromClause(table1, null), new FromClause(table2, null));

                params.put("from", fromClause);
            }

            addJoinConditions(fromClause, ctx.join_clause());

            Conditions conditions = new Conditions();
            for (SQLiteParser.ExprContext expression : ctx.expr()) {
                conditions = getCondition(fromClause, expression);
            }
            params.put("conditions", conditions);

            List<SQLiteParser.Result_columnContext> tempList = ctx.result_column();
            for (SQLiteParser.Result_columnContext result_columnContext : tempList) {
                SQLiteParser.Table_nameContext table_nameContext = result_columnContext.table_name();
                String columnName = result_columnContext.expr().column_name().getText();
                if (table_nameContext != null) {
                    Table columnTable = tableManager.getTable(table_nameContext.getText());
                    if (columnTable == null)
                        throw new QueryException(String.format("No table '%s' found", table_nameContext.getText()));
                    if (fillColumnTable(fromClause, columnTable, columnName))
                        throw new QueryException(String.format("No column '%s' found", columnName));
                } else {
                    if (fillColumnTable(fromClause, columnName) == null)
                        throw new QueryException(String.format("No column '%s' found", columnName));
                }
            }
            params.put("from", fromClause);
        } catch (Exception e) {
            statementType = StatementType.NONE;
            error_message = e.getMessage();
        }
    }

    private void addJoinConditions(FromClause fromClause, SQLiteParser.Join_clauseContext joinCtx) {
        if (joinCtx != null)
            for (SQLiteParser.Join_constraintContext joinConstraintCtx : joinCtx.join_constraint()) {
                List<SQLiteParser.ExprContext> onCondition = joinConstraintCtx.expr().expr();
                String firstTableName = onCondition.get(0).table_name().getText();
                String secondTableName = onCondition.get(1).table_name().getText();
                String firstColumnName = onCondition.get(0).column_name().getText();
                String secondColumnName = onCondition.get(1).column_name().getText();
                if (fromClause.getTable() == null)
                    fromClause.addTableJoinCondition(firstTableName, secondTableName, firstColumnName, secondColumnName);
            }
    }

    private boolean fillColumnTable(FromClause fromClause, Table table, String columnName) {
        if (fromClause.getTable() == table) {
            Column foundColumn = fromClause.getTable().getColumn(columnName);
            if (foundColumn != null)
                fromClause.addColumn(foundColumn);
            return foundColumn != null;
        } else {
            boolean firstFound = fillColumnTable(fromClause.getFirstFrom(), table, columnName);
            boolean secondFound = fillColumnTable(fromClause.getSecondFrom(), table, columnName);
            return firstFound || secondFound;
        }

    }


    private Table fillColumnTable(FromClause fromClause, String columnName) throws QueryException {
        if (fromClause.getTable() != null) {
            Column foundColumn = fromClause.getTable().getColumn(columnName);
            if (foundColumn != null) {
                fromClause.addColumn(foundColumn);
                return fromClause.getTable();
            }
            return null;
        } else {
            Table firstFound = fillColumnTable(fromClause.getFirstFrom(), columnName);
            Table secondFound = fillColumnTable(fromClause.getSecondFrom(), columnName);
            if (firstFound != null && secondFound != null && firstFound != secondFound)
                throw new QueryException(String.format("Ambiguous in column '%s' name: table %s or %s",
                        columnName, firstFound.getName(), secondFound.getName()));
            if (firstFound != null)
                return firstFound;
            if (secondFound != null)
                return secondFound;
            return null;
        }
    }

    private Conditions getCondition(FromClause fromClause, SQLiteParser.ExprContext expression) throws QueryException {
        Conditions result = new Conditions();

        String columnName;
        String comparison = expression.children.get(1).getText();

        if (comparison.toUpperCase().equals("AND")) {
            result.addValues(getCondition(fromClause, (SQLiteParser.ExprContext) expression.children.get(0)));
            result.addValues(getCondition(fromClause, (SQLiteParser.ExprContext) expression.children.get(2)));
            return result;
        }

        ComparisonType comparisonType = ComparisonType.fromString(comparison);

        if (expression.expr(0).column_name() != null)
            columnName = expression.expr(0).column_name().getText();
        else {
            columnName = expression.expr(0).expr(0).column_name().getText();
        }
        Pair<Table, Column> findRes = fromClause.getTableColumn(columnName);
        Column column = findRes.b;
        if (column == null)
            throw new QueryException(String.format("No column '%s' find", columnName));

        String valueText = expression.expr(1).getText();
        Object value = column.getType().castFromString(valueText);
        if (value == null)
            throw new QueryException(String.format("Invalid value '%s' at column '%s', use type %s",
                    valueText, columnName, column.getType().getName()));

        result.addValue(new Condition(findRes.a, column, comparisonType, value));
        return result;
    }

    @Override
    public void enterSimple_select_stmt(SQLiteParser.Simple_select_stmtContext ctx) {

    }

    @Override
    public void exitSimple_select_stmt(SQLiteParser.Simple_select_stmtContext ctx) {
    }

    @Override
    public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {

    }

    @Override
    public void exitSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
    }


    @Override
    public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {

    }

    @Override
    public void exitInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {
        statementType = StatementType.INSERT;
        try {
            Table table = tableManager.getTable(ctx.table_name().getText());
            params.put("table_name", table.getName());

            List<Column> columns = new ArrayList<>();
            List<SQLiteParser.Column_nameContext> tempList = ctx.column_name();
            for (SQLiteParser.Column_nameContext column_nameContext : tempList) {
                String columnName = column_nameContext.any_name().getText();
                Column column = table.getColumn(columnName);
                if (column == null)
                    throw new QueryException(String.format("Invalid column '%s' in table '%s'", columnName, table.getName()));
                columns.add(column);
            }
            params.put("columns", columns);

            Conditions conditions = new Conditions();
            for (int i = 0; i < columns.size(); i++) {
                SQLiteParser.ExprContext expression = ctx.expr(i);
                Column column = columns.get(i);
                ComparisonType comparisonType = ComparisonType.EQUAL;

                String valueText = expression.getText();
                Object value = column.getType().castFromString(valueText);
                if (value == null)
                    throw new QueryException(String.format("Invalid value '%s' at column '%s', use type %s",
                            valueText, column.getName(), column.getType().getName()));

                conditions.addValue(new Condition(table, column, comparisonType, value));
            }
            params.put("conditions", conditions);


        } catch (QueryException e) {
            statementType = StatementType.NONE;
            error_message = e.getMessage();
        }
    }

    @Override
    public void exitDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
        statementType = StatementType.DELETE;
        try {
            Table table = tableManager.getTable(ctx.qualified_table_name().getText());
            params.put("table_name", table.getName());
            Conditions conditions = getCondition(new FromClause(table, null), ctx.expr());
            params.put("conditions", conditions);
        } catch (QueryException e) {
            statementType = StatementType.NONE;
            error_message = e.getMessage();
        }

    }


    @Override public void exitUpdate_stmt(SQLiteParser.Update_stmtContext ctx) {
        statementType = StatementType.UPDATE;
        try {
            Table table = tableManager.getTable(ctx.qualified_table_name().getText());
            params.put("table_name", table.getName());

            Conditions assignments = new Conditions();

            List<SQLiteParser.Column_nameContext> setColumnNames = ctx.column_name();
            for (int i = 0; i < setColumnNames.size(); i++) {
                SQLiteParser.Column_nameContext column_nameContext = setColumnNames.get(i);
                Column column = table.getColumn(column_nameContext.getText());
                String valueText = ctx.expr(i).getText();
                Object value = column.getType().castFromString(valueText);
                if (value == null)
                        throw new QueryException(String.format("Invalid value '%s' at column '%s', use type %s",
                                valueText, column.getName(), column.getType().getName()));
                Condition condition = new Condition(table, column, ComparisonType.EQUAL, value);
                assignments.addValue(condition);
            }
            params.put("assignments", assignments);

            for (int i = setColumnNames.size(); i < ctx.expr().size(); i++) {
                FromClause fromClause = new FromClause(table, null);
                params.put("conditions", getCondition(fromClause, ctx.expr(i)));
            }

        } catch (QueryException e) {
            e.printStackTrace();
        }


    }

}
