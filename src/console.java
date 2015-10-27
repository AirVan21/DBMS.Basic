import commands_runner.TableManager;
import common.Column;
import common.Statement;
import common.Type;
import common.BaseType;
import parser.SQLParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 06.10.15.
 */

public class console {

    public static void main( final String[] args ){
        String dbFolder = "data//";
        Integer bufferPoolSize = 512;
        TableManager tableManager = new TableManager(bufferPoolSize, dbFolder);
        SQLParser sqlParser = new SQLParser();
        System.out.println("Hello DBMS!");

        testXML(tableManager);
        testParser(tableManager, sqlParser);
    }

    private static void testParser(TableManager tableManager, SQLParser sqlParser) {
        String query = "CREATE TABLE database_name.new_table(column1 INTEGER);";
        Statement statement = sqlParser.parse(query);
        switch (statement.getType()) {
            case CREATE:
                tableManager.createTable(statement.getStringParam("table_name"),
                          (List<Column>) statement.getParam("columns"));
                break;
            case SELECT:
                break;
            case INSERT:
                break;
        }
    }

    public static void testXML(TableManager tableManager)
    {
        Type type = new Type(BaseType.INT);
        Column ageColumn = new Column("Age", type);
        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        tableManager.createTable("person", columns);
    }

}