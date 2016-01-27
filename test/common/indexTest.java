package common;

import commands_runner.TableManager;
import commands_runner.cursors.ICursor;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import parser.SQLParser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by airvan21 on 27.01.16.
 */
public class indexTest {

    private TableManager manager = null;
    private final String dbFolder = "dataTest//";

    @Before
    public void beforeTest() {
        deleteDirectory(new File(dbFolder));
        Integer bufferPoolSize = 16;
        manager = new TableManager(bufferPoolSize, dbFolder);
    }

    @Test()
    public void createIndexTest() {
        String tableName = "testTable";
        Column ageColumn = new Column("Age", new Type(BaseType.INT));
        Column nameColumn = new Column("Name", Type.createType("varchar", 20));
        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        columns.add(nameColumn);
        assertTrue(manager.createTable(tableName, columns));
        Column column = manager.getTable(tableName).getColumns().get(0);
        assertNotNull(column);

        SQLParser sqlParser = new SQLParser(manager);
        final int insertCount = 4500;
        for (int i = 0; i < insertCount; i++) {
            String query = "Insert into db." + tableName + " (name, age) values (\"Petr\", " + i * 10 + ")";
            runInsert(sqlParser, query);
        }

        manager.createIndex(tableName, column);

        for (int i = 0; i < insertCount / 100; i++) {
            String query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s " +
                    "where age > %2$d and age <= %3$d", tableName, i * 100, i * 100 + 20);
            int count = runSelect(sqlParser, query);
            if (count != 2)
                break;
            assertEquals(2, count);
        }
    }

    @After
    public void afterTest() {
        try {
            manager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean deleteDirectory(File directory) {
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

    private void runInsert(SQLParser sqlParser, String query) {
        try {
            Statement statement = sqlParser.parse(query);
            manager.insert(statement.getStringParam("table_name"),
                    (Conditions) statement.getParam("conditions"));
        } catch (QueryException e) {
            e.printStackTrace();
        }
    }

    private int runSelect(SQLParser sqlParser, String query) {
        try {
            Statement statement = sqlParser.parse(query);
            ICursor cursor = manager.select(statement.getStringParam("table_name"),
                    (List<ColumnSelect>) statement.getParam("columns"),
                    (Conditions) statement.getParam("conditions"));

            int counter = 0;

            System.out.println("=== SELECT ===");
            while (cursor.next()) {
                counter++;
                if (counter % 100 == 0) {
                    System.out.println(counter);
                }
            }

            return counter;
        } catch (QueryException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
