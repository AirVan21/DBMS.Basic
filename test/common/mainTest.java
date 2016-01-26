package common;

import commands_runner.TableManager;
import commands_runner.cursors.ICursor;
import common.conditions.Conditions;
import common.exceptions.QueryException;
import common.table_classes.Table;
import org.junit.*;
import parser.SQLParser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by semionn on 28.12.15.
 */
public class mainTest {

    private TableManager manager = null;
    private final String dbFolder = "dataTest//";

    @Before
    public void beforeTest() {
        deleteDirectory(new File(dbFolder));
        Integer bufferPoolSize = 32;
        manager = new TableManager(bufferPoolSize, dbFolder);
    }

    @Test
    public void createTableTest() {
        String tableName = "testTable";
        assertTrue(manager.createTable(tableName, new ArrayList<Column>()));

        Table table = manager.getTable(tableName);
        assertNotNull(table);

        assertTrue(new File(table.getFileName()).exists());
    }

    @Test
    public void createTableDuplicateTest() {
        String tableName = "testTable";
        // Creates first table
        assertTrue(manager.createTable(tableName, new ArrayList<Column>()));
        // Refuse duplication
        assertFalse(manager.createTable(tableName, new ArrayList<Column>()));
        // Still have information about first table
        Table table = manager.getTable(tableName);
        assertNotNull(table);

        assertTrue(new File(table.getFileName()).exists());
    }

    @Test
    public void createTableRightColumnsTest() {
        final String age = "Age";
        final String name = "Name";
        final String salary = "Salary";
        Column ageColumn = new Column(age, new Type(BaseType.INT));
        Column nameColumn = new Column(name, Type.createType("varchar", 20));
        Column salaryColumn = new Column(salary, new Type(BaseType.DOUBLE));

        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        columns.add(nameColumn);
        columns.add(salaryColumn);

        String tableName = "testTable";
        assertTrue(manager.createTable(tableName, columns));

        Table table = manager.getTable(tableName);
        assertNotNull(table);

        assertEquals(table.getName(), tableName);
        assertNotNull(table.getColumn(age));
        assertNotNull(table.getColumn(name));
        assertNotNull(table.getColumn(salary));

        assertEquals(table.getColumn(age).getType().baseType, BaseType.INT);
        assertEquals(table.getColumn(name).getType().baseType, BaseType.VARCHAR);
        assertEquals(table.getColumn(salary).getType().baseType, BaseType.DOUBLE);
    }

    @Test
    public void createTableWrongColumnsTest() {
        final String age = "Age";
        final String name = "Name";
        final String salary = "Salary";
        final String weight = "Weight";

        Column ageColumn = new Column(age, new Type(BaseType.INT));
        Column nameColumn = new Column(name, Type.createType("varchar", 40));
        Column weightColumn = new Column(weight, new Type(BaseType.DOUBLE));

        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        columns.add(nameColumn);
        columns.add(weightColumn);

        String tableName = "testTable";
        assertTrue(manager.createTable(tableName, columns));
        assertNull(manager.getTable(tableName + salary));

        Table table = manager.getTable(tableName);
        assertNotEquals(table.getName(), tableName + salary);

        assertNull(table.getColumn(salary));
        assertNotNull(table.getColumn(weight));

        assertNotEquals(table.getColumn(age).getType().baseType, BaseType.VARCHAR);
        assertNotEquals(table.getColumn(name).getType().baseType, BaseType.DOUBLE);
        assertNotEquals(table.getColumn(weight).getType().baseType, BaseType.INT);
    }

    @Test
    public void insertRightTest() {
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);
        String query = "Insert into db." + tableName + " (name, age, salary) values (\"Petr\", 22, 23504.5)";
        runInsert(sqlParser, query);
        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.age = 22", tableName);
        int count = runSelect(sqlParser, query);
        assertEquals(1, count);
    }

    @Test
    public void insertRightTestBigSameRecords() {
        final int TEST_SIZE = 1000; // x2 different records
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);
        String queryFirst = "Insert into db." + tableName + "(name, age, salary) values (\"Petr\", 22, 23504.5)";
        String querySecond = "Insert into db." + tableName + "(name, age, salary) values (\"Ann\", 35, 47412.0)";
        Statement statementFirst = createStatement(sqlParser, queryFirst);
        Statement statementSecond = createStatement(sqlParser, querySecond);

        for (int i = 0; i < TEST_SIZE; ++i) {
            manager.insert(statementFirst.getStringParam("table_name"), (Conditions) statementFirst.getParam("conditions"));
            manager.insert(statementSecond.getStringParam("table_name"), (Conditions) statementSecond.getParam("conditions"));
        }

        int count = 0;

        String queryCountFirst  = String.format("Select %1$s.age, %1$s.name from db.%1$s where %1$s.salary < 25000.0", tableName);
        count = runSelect(sqlParser, queryCountFirst);
        assertEquals(TEST_SIZE, count);

        String queryCountSecond = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.name = \"Ann\"", tableName);
        count = runSelect(sqlParser, queryCountSecond);
        assertEquals(TEST_SIZE, count);

        String queryCountAll  = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1%s.age > 20", tableName);
        count = runSelect(sqlParser, queryCountAll);
        assertEquals(TEST_SIZE * 2, count);
    }

    @Test
    public void insertRightTestDifferentRecords() {
        final int TEST_SIZE = 1000; // x2 different records
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);

        double salary = 100;
        for (int i = 0; i < TEST_SIZE; ++i) {
            String query = String.format("Insert into db.%1$s (name, age, salary) values (\"Ann\", 35, %2$f)", tableName, salary + 5 * i);
            runInsert(sqlParser, query);
        }

        int count = 0;

        String queryCountFirst  = String.format("Select %1$s.age, %1$s.name from db.%1$s where %1$s.salary >= 100.0 and %1$s.age = 35", tableName);
        count = runSelect(sqlParser, queryCountFirst);
        assertEquals(TEST_SIZE, count);

        String queryCountSecond = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.salary >=130 and %1$s.salary <= 150", tableName);
        count = runSelect(sqlParser, queryCountSecond);
        assertEquals(5, count);

    }

    @Test(expected = Exception.class)
    public void insertWrongTableTest() {
        String tableName = "testTable";
        manager.insert(tableName, new Conditions());
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


        final int default_age = 25;
        SQLParser sqlParser = new SQLParser(manager);
        String queryInsert = "Insert into db." + tableName + " (name, age) values (\"Petr\", " + default_age + ")";
        Statement statement = createStatement(sqlParser, queryInsert);

        final int insertCount = 100;
        for (int i = 0; i < insertCount; i++) {
            manager.insert(statement.getStringParam("table_name"),
                    (Conditions) statement.getParam("conditions"));
        }

        manager.createIndex(tableName, column);


        for (int i = 0; i < 20; i++) {
            String query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s " +
                    "where age > %2$d and age <= %3$d", tableName, i * 100, i * 100 + 20);
            int count = runSelect(sqlParser, query);
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
            while (cursor.next()) {
                counter++;
                System.out.println(cursor.getRecord());
            }

            return counter;
        } catch (QueryException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private Statement createStatement(SQLParser sqlParser, String query)
    {
        Statement statement = null;
        try {
            statement = sqlParser.parse(query);
        } catch (QueryException e) {
            e.printStackTrace();
        }

        return statement;
    }
}
