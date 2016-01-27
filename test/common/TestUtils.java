package common;

import commands_runner.TableManager;
import commands_runner.cursors.ICursor;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Table;
import parser.SQLParser;

import java.io.File;
import java.util.List;

/**
 * Created by airvan21 on 27.01.16.
 */
public class TestUtils {

    public static void runInsert(TableManager manager, SQLParser sqlParser, String query) {
        try {
            Statement statement = sqlParser.parse(query);
            manager.insert(statement.getStringParam("table_name"),
                    (Conditions) statement.getParam("conditions"));
        } catch (QueryException e) {
            e.printStackTrace();
        }
    }

    public static int runSelect(TableManager manager, SQLParser sqlParser, String query, Integer printFrequency) {
        try {
            if (printFrequency == null)
                printFrequency = 100;

            Statement statement = sqlParser.parse(query);
            ICursor cursor = manager.select(statement.getStringParam("table_name"),
                    (List<ColumnSelect>) statement.getParam("columns"),
                    (Conditions) statement.getParam("conditions"));

            int counter = 0;

            System.out.println("=== SELECT ===");
            while (cursor.next()) {
                counter++;
                if (counter % printFrequency == 0) {
                    System.out.println(counter);
                }
            }
            return counter;
        } catch (QueryException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static int runDelete(TableManager manager, SQLParser sqlParser, String query) {
        try {
            Statement statement = sqlParser.parse(query);
            int count = manager.delete(statement.getStringParam("table_name"),
                    (Conditions) statement.getParam("conditions"));
            System.out.println(count);
            return count;
        } catch (QueryException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static Statement createStatement(SQLParser sqlParser, String query)
    {
        Statement statement = null;
        try {
            statement = sqlParser.parse(query);
        } catch (QueryException e) {
            e.printStackTrace();
        }

        return statement;
    }

    public static boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();

            if (files == null)
                return true;

            for (File file : files) {
                if (file.isDirectory())
                    deleteDirectory(file);
                else
                    file.delete();
            }
        }
        return directory.delete();
    }
}
