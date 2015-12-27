package common;

import commands_runner.TableManager;
import common.table_classes.Table;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

/**
 * Created by semionn on 28.12.15.
 */
public class mainTest {

    //Test example
    @Test
    public void mainTest() {
        String dbFolder = "dataTest//";
        Integer bufferPoolSize = 16;
        try (TableManager tableManager = new TableManager(bufferPoolSize, dbFolder))
        {
            String tableName = "testTable";
            tableManager.createTable(tableName, new ArrayList<Column>());
            Table table = tableManager.getTable(tableName);
            assertTrue(new File(table.getFileName()).exists());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
