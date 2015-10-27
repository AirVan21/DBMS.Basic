package parser.sql_antlr;

import common.BaseType;
import common.Column;
import common.StatementType;
import common.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by semionn on 27.10.15.
 */
public class SQL_Listener extends SQLiteBaseListener {

    StatementType statementType = StatementType.NONE;

    Map<String,Object> params = new HashMap<>();

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
}
