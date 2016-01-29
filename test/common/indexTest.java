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

    private final boolean UPDATE_FLAG = false;
    private TableManager manager = null;
    private final String dbFolder = "dataTest//";

    @Before
    public void beforeTest() {
        if (UPDATE_FLAG) {
            TestUtils.deleteDirectory(new File(dbFolder));
        }
        Integer bufferPoolSize = 32;
        manager = new TableManager(bufferPoolSize, dbFolder);
    }

    @Test()
    public void createIndexTest() {
        final int step = 10;
        final int insertCount = 100_000; // TEST_SIZE
        String tableName = "testTable";

        // If you want to recreate table & index => set "UPDATE_FLAG = true;"
        if (UPDATE_FLAG) {
            createIndexWorkaround(insertCount, tableName);
        } else {
            manager.loadTables();
        }

        final int range = 10;
        SQLParser sqlParser = new SQLParser(manager);
        for (int i = 0;  i < insertCount / 100; i++) {
             String query = String.format("Select %1$s.age, %1$s.name from db.%1$s " +
                    "where age > %2$d and age <= %3$d", tableName, i * 10, i * 10 + range * step);
            int count = TestUtils.runSelect(manager, sqlParser, query, null);
            assertEquals(range, count);
        }
    }

    public void createIndexWorkaround(int testSize, String nameTable) {
        String tableName = nameTable;
        Column ageColumn = new Column("Age", new Type(BaseType.INT));
        Column nameColumn = new Column("Name", Type.createType("varchar", 20));
        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        columns.add(nameColumn);
        manager.createTable(tableName, columns);
        Column column = manager.getTable(tableName).getColumns().get(0);

        SQLParser sqlParser = new SQLParser(manager);
        String query = "Insert into db." + tableName + " (name, age) values (\"Petr\", 0)";
        Statement statement = TestUtils.createStatement(sqlParser, query);

        final int insertCount = testSize; // TEST_SIZE
        final int step = 10;

        for (int i = 0; i < insertCount; i++) {
            ((Conditions) statement.getParam("conditions")).getValues().get(1).setValue((Comparable<Object>) (Object) (i * step));
            manager.insert(statement.getStringParam("table_name"), (Conditions) statement.getParam("conditions"));
        }

        manager.createIndex(tableName, column);
    }

    @After
    public void afterTest() {
        try {
            manager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
