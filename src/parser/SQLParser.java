package parser;

import commands_runner.ITableManager;
import common.Statement;
import common.StatementType;
import common.exceptions.QueryException;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import parser.sql_antlr.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Created by semionn on 27.10.15.
 */
public class SQLParser implements ISQLParser{

    ITableManager tableManager;

    public SQLParser(ITableManager tableManager) {
        this.tableManager = tableManager;
    }

    public Statement parse(String query) throws QueryException{
        InputStream is = new ByteArrayInputStream( query.getBytes());
        try {
            CharStream charStream = new ANTLRInputStream(is);
            SQLiteLexer sqLiteLexer = new SQLiteLexer(charStream);
            CommonTokenStream tokens = new CommonTokenStream(sqLiteLexer);
            SQLiteParser parser = new SQLiteParser(tokens);
            SQLListener sql_listener = new SQLListener(tableManager);
            parser.addParseListener(sql_listener);
            parser.parse();
            if (sql_listener.getStatementType() == StatementType.NONE)
                throw new QueryException(sql_listener.error_message);
            return new Statement(sql_listener.getStatementType(), sql_listener.getParams());
        } catch (IOException e){

        }
        return null;
    }
}
