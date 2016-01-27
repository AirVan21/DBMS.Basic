package common;

import commands_runner.TableManager;
import common.table_classes.Table;
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
public class joinTest {

    private TableManager manager = null;
    private final String dbFolder = "dataTest//";

    @Before
    public void beforeTest() {
        TestUtils.deleteDirectory(new File(dbFolder));
        Integer bufferPoolSize = 16;
        manager = new TableManager(bufferPoolSize, dbFolder);
    }

    @Test
    public void createTableFirstTest() {
        final String id = "id";
        final String name = "name";
        final String salary = "salary";

        Column idColumn = new Column(id, new Type(BaseType.INT));
        Column nameColumn = new Column(name, Type.createType("varchar", 20));
        Column salaryColumn = new Column(salary, new Type(BaseType.DOUBLE));

        List<Column> columns = new ArrayList<>();
        columns.add(idColumn);
        columns.add(nameColumn);
        columns.add(salaryColumn);

        String tableName = "firstTest";
        assertTrue(manager.createTable(tableName, columns));

        Table table = manager.getTable(tableName);
        assertNotNull(table);

        assertEquals(table.getName(), tableName);
        assertNotNull(table.getColumn(id));
        assertNotNull(table.getColumn(name));
        assertNotNull(table.getColumn(salary));

        assertEquals(table.getColumn(id).getType().baseType, BaseType.INT);
        assertEquals(table.getColumn(name).getType().baseType, BaseType.VARCHAR);
        assertEquals(table.getColumn(salary).getType().baseType, BaseType.DOUBLE);
    }

    @Test
    public void createTableSecondTest() {
        final String id = "id";
        final String idfst = "idfst";
        final String parking = "parking";

        Column idColumn = new Column(id, new Type(BaseType.INT));
        Column idfstCoulmn = new Column(idfst, new Type(BaseType.INT));
        Column parkingColumn = new Column(parking, new Type(BaseType.INT));

        List<Column> columns = new ArrayList<>();
        columns.add(idColumn);
        columns.add(idfstCoulmn);
        columns.add(parkingColumn);

        String tableName = "secondTest";
        assertTrue(manager.createTable(tableName, columns));

        Table table = manager.getTable(tableName);
        assertNotNull(table);

        assertEquals(table.getName(), tableName);
        assertNotNull(table.getColumn(id));
        assertNotNull(table.getColumn(idfst));
        assertNotNull(table.getColumn(parking));

        assertEquals(table.getColumn(id).getType().baseType, BaseType.INT);
        assertEquals(table.getColumn(idfst).getType().baseType, BaseType.INT);
        assertEquals(table.getColumn(parking).getType().baseType, BaseType.INT);
    }

    @Test
    public void simpleJoinTest() {
        createTableFirstTest();
        createTableSecondTest();
        String firstTableName  = "firstTest";
        String secondTableName = "secondTest";
        SQLParser sqlParser = new SQLParser(manager);

        String queryFirst = String.format("Insert into db.%1$s (id, name, salary) values (1, \"Ann\", 15000)", firstTableName);
        TestUtils.runInsert(manager, sqlParser, queryFirst);
        queryFirst = String.format("Insert into db.%1$s (id, name, salary) values (2, \"Jane\", 25000)", firstTableName);
        TestUtils.runInsert(manager, sqlParser, queryFirst);
        queryFirst = String.format("Insert into db.%1$s (id, name, salary) values (3, \"Harry\", 35000)", firstTableName);
        TestUtils.runInsert(manager, sqlParser, queryFirst);
        queryFirst = String.format("Insert into db.%1$s (id, name, salary) values (4, \"Michael\", 28000)", firstTableName);
        TestUtils.runInsert(manager, sqlParser, queryFirst);

        String querySecond = String.format("Insert into db.%1$s (id, idfst, parking) values (1, 1, 7)", secondTableName);
        TestUtils.runInsert(manager, sqlParser, querySecond);
        querySecond = String.format("Insert into db.%1$s (id, idfst, parking) values (2, 4, 8)", secondTableName);
        TestUtils.runInsert(manager, sqlParser, querySecond);
        querySecond = String.format("Insert into db.%1$s (id, idfst, parking) values (3, 4, 2)", secondTableName);
        TestUtils.runInsert(manager, sqlParser, querySecond);
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
