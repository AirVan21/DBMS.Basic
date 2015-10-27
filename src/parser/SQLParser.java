package parser;

import common.Statement;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Vocabulary;
import parser.sql_antlr.*;
import sun.misc.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
 * Created by semionn on 27.10.15.
 */
public class SQLParser implements ISQLParser{

    public Statement parse(String query) {
        InputStream is = new ByteArrayInputStream( query.getBytes());
        try {
            CharStream charStream = new ANTLRInputStream(is);
            SQLiteLexer sqLiteLexer = new SQLiteLexer(charStream);
            CommonTokenStream tokens = new CommonTokenStream(sqLiteLexer);
            SQLiteParser parser = new SQLiteParser(tokens);
            SQL_Listener sql_listener = new SQL_Listener();
            parser.addParseListener(sql_listener);
            parser.parse();
            return new Statement(sql_listener.getStatementType(), sql_listener.getParams());
        } catch (Exception e){

        }
        return null;
    }
}
