package common;

import commands_runner.TableManager;
import common.table_classes.Table;
import org.junit.*;

import java.io.File;
import java.util.ArrayList;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by semionn on 28.12.15.
 */
public class mainTest {

    TableManager manager = null;

    public static boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();

            if (files == null)
                return  true;

            for (File file: files) {
                if (file.isDirectory())
                    deleteDirectory(file);
                else
                    file.delete();
            }
        }

        return directory.delete();
    }

    @Before
    public void beforeMainTest() {
        String dbFolder = "dataTest//";
        deleteDirectory(new File(dbFolder));

        Integer bufferPoolSize = 16;
        manager = new TableManager(bufferPoolSize, dbFolder);
    }

    @Test
    public void createTableTest() {

        String tableName = "testTable";
        assertTrue(manager.createTable(tableName, new ArrayList<Column>()));

        assertNotNull(manager.getTable(tableName));
        Table table = manager.getTable(tableName);

        assertTrue(new File(table.getFileName()).exists());
    }

    @After
    public void afterMainTest() {
        try {
            manager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
