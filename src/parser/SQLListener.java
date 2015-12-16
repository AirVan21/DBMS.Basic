package parser;

import commands_runner.ITableManager;
import commands_runner.TableManager;
import common.*;
import common.conditions.ComparisonType;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Table;
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
            params.put("join", ctx.join_clause());
            Table table = tableManager.getTable(ctx.table_or_subquery(0).table_name().getText());
            params.put("table_name", table.getName());

            Conditions conditions = new Conditions();
            for (SQLiteParser.ExprContext expression : ctx.expr()) {
                String columnName = "";
                Integer depth = 0;
                if (expression.expr(0).column_name() != null)
                    columnName = expression.expr(0).column_name().getText();
                else {
                    columnName = expression.expr(0).expr(0).column_name().getText();
                    depth = 1;
                }
                Column column = table.getColumn(columnName);
                if (column == null)
                    throw new QueryException(String.format("No column '%s' find in table '%s'", table.getName(), columnName));


                String comparison = expression.children.get(1).getText();
                ComparisonType comparisonType = ComparisonType.fromString(comparison);


                String valueText = expression.expr(1).getText();
                Object value = column.getType().castFromString(valueText);
                if (value == null)
                    throw new QueryException(String.format("Invalid value '%s' at column '%s', use type %s",
                            valueText, columnName, column.getType().getName()));

                conditions.addValue(new Condition(table, column, comparisonType, value));
            }
            params.put("conditions", conditions);

//            List<ColumnSelect> columns = new ArrayList<>();
//            List<SQLiteParser.Result_columnContext> tempList = ctx.result_column();
//            for (SQLiteParser.Result_columnContext result_columnContext : tempList) {
//                SQLiteParser.Table_nameContext table_nameContext = result_columnContext.table_name();
//                Table columnTable = table;
//                if (table_nameContext != null) {
//                    columnTable = tableManager.getTable(table_nameContext.getText());
//                    if (columnTable == null)
//                        throw new QueryException(String.format("No table '%s' found", table_nameContext.getText()));
//                }
//                String columnName = result_columnContext.expr().column_name().getText();
//                Column column = columnTable.getColumn(columnName);
//                columns.add(new ColumnSelect(columnTable, column));
//            }
//            params.put("columns", ctx.result_column());
        } catch (Exception e) {
            statementType = StatementType.NONE;
            error_message = e.getMessage();
        }
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


        } catch (Exception e) {
            statementType = StatementType.NONE;
            error_message = e.getMessage();
        }
    }

}
