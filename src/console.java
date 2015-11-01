import commands_runner.TableManager;
import common.*;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import parser.SQLParser;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
        SQLParser sqlParser = new SQLParser(tableManager);
        System.out.println("Hello DBMS!");

        testXML(tableManager);
        testParser(tableManager, sqlParser);
    }

    private static void testParser(TableManager tableManager, SQLParser sqlParser) {
        //String query = "CREATE TABLE database_name.new_table(column1 INTEGER);";
        String query = "Select person.age, person.name from db.person where person.age > 5";
        try {
            Statement statement = sqlParser.parse(query);
            switch (statement.getType()) {
                case CREATE:
                    tableManager.createTable(statement.getStringParam("table_name"),
                            (List<Column>) statement.getParam("columns"));
                    break;
                case SELECT:
                    tableManager.select(statement.getStringParam("table_name"),
                            (List<ColumnSelect>) statement.getParam("columns"),
                            (Conditions) statement.getParam("conditions"));
                    break;
                case INSERT:
                    break;
            }
        } catch (QueryException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void testXML(TableManager tableManager)
    {
        Type type = new Type(BaseType.INT);
        Column ageColumn = new Column("Age", type);
        Column nameColumn = new Column("Name", Type.createType("varchar", 20));
        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        columns.add(nameColumn);
        tableManager.createTable("person", columns);
    }

}