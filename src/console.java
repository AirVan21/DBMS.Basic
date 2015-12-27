import commands_runner.TableManager;
import commands_runner.cursors.ICursor;
import common.*;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import parser.SQLParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 06.10.15.
 */

public class console {

    public static void main( final String[] args ){
        String dbFolder = "data//";
        Integer bufferPoolSize = 16;
        try (TableManager tableManager = new TableManager(bufferPoolSize, dbFolder)) {
            tableManager.loadTables();
            SQLParser sqlParser = new SQLParser(tableManager);
            System.out.println("Hello DBMS!");
            String query;
//            createTableTest(tableManager);
//            query = "Insert into db.person (name, age) values (\"Petr\", 22)";
//            runQuery(tableManager, sqlParser, query);
//            for (int i = 0; i < 10_000; i++) {
//                insertTest(tableManager, sqlParser, i);
//                if (i % 100 == 0)
//                    System.out.println(String.format("%d inserted", i));
//            }
//            query = "Select person.age, person.name from db.person where person.name = \"Petr\"";
//            runQuery(tableManager, sqlParser, query);
            query = "Select person.age, person.name from db.person where person.age >= 0";
            runQuery(tableManager, sqlParser, query);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runQuery(TableManager tableManager, SQLParser sqlParser, String query) {
        try {
            Statement statement = sqlParser.parse(query);
            switch (statement.getType()) {
                case CREATE:
                    tableManager.createTable(statement.getStringParam("table_name"),
                            (List<Column>) statement.getParam("columns"));
                    break;
                case SELECT:
                    ICursor cursor = tableManager.select(statement.getStringParam("table_name"),
                            (List<ColumnSelect>) statement.getParam("columns"),
                            (Conditions) statement.getParam("conditions"));
                    int counter = 0;
                    while (cursor.next()) {
                        System.out.println(cursor.getRecord().values);
                        counter++;
                        if (counter % 1000 == 0)
                            System.out.println(counter);
                    }
                    System.out.println(counter);
                    break;
                case INSERT:
                    tableManager.insert(statement.getStringParam("table_name"),
                            (Conditions) statement.getParam("conditions"));
                    break;
            }
        } catch (QueryException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void createTableTest(TableManager tableManager)
    {
        Type type = new Type(BaseType.INT);
        Column ageColumn = new Column("Age", type);
        Column nameColumn = new Column("Name", Type.createType("varchar", 20));
        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        columns.add(nameColumn);
        tableManager.createTable("person", columns);
    }

    public static void insertTest(TableManager tableManager, SQLParser sqlParser, int num)
    {
        String[] names = { "Петя"};
        for (String name : names) {
            String query = String.format("Insert into db.person (name, age) values (\"%s\", %s)", name, num);
            runQuery(tableManager, sqlParser, query);
        }
    }

}