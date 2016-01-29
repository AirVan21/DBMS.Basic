package common;

import commands_runner.TableManager;
import common.conditions.Conditions;
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
        TestUtils.deleteDirectory(new File(dbFolder));
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
        TestUtils.runInsert(manager, sqlParser, query);

        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.age = 22", tableName);
        int count = TestUtils.runSelect(manager, sqlParser, query, null);
        assertEquals(1, count);

        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.name = \"Petr\" and %1$s.salary = 23504.5" , tableName);
        count = TestUtils.runSelect(manager, sqlParser, query, null);
        assertEquals(1, count);
    }

    @Test
    public void insertRightTestBigSameRecords() {
        final int TEST_SIZE = 50_000; // x2 same records
        final int frequency = 1000;
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);
        String queryFirst = "Insert into db." + tableName + "(name, age, salary) values (\"Petr\", 22, 23504.5)";
        String querySecond = "Insert into db." + tableName + "(name, age, salary) values (\"Ann\", 35, 47412.0)";
        Statement statementFirst = TestUtils.createStatement(sqlParser, queryFirst);
        Statement statementSecond = TestUtils.createStatement(sqlParser, querySecond);

        for (int i = 0; i < TEST_SIZE; ++i) {
            manager.insert(statementFirst.getStringParam("table_name"), (Conditions) statementFirst.getParam("conditions"));
            manager.insert(statementSecond.getStringParam("table_name"), (Conditions) statementSecond.getParam("conditions"));
        }

        String queryCountFirst  = String.format("Select %1$s.age, %1$s.name from db.%1$s where %1$s.salary < 25000.0", tableName);
        int count = TestUtils.runSelect(manager, sqlParser, queryCountFirst, frequency);
        assertEquals(TEST_SIZE, count);

        String queryCountSecond = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.name = \"Ann\"", tableName);
        count = TestUtils.runSelect(manager, sqlParser, queryCountSecond, frequency);
        assertEquals(TEST_SIZE, count);

        String queryCountAll  = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.age > 20", tableName);
        count = TestUtils.runSelect(manager, sqlParser, queryCountAll, frequency);
        assertEquals(TEST_SIZE * 2, count);
    }

    @Test
    public void insertRightTestDifferentRecords() {
        final int TEST_SIZE = 10_000; // different records
        final int frequency = 1000;
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);

        double default_salary = 10_000;
        for (int i = 0; i < TEST_SIZE; ++i) {
            String query = String.format("Insert into db.%1$s (name, age, salary) values (\"Ann\", 35, %2$f)", tableName, default_salary + i);
            TestUtils.runInsert(manager, sqlParser, query);
        }

        String queryCountFirst  = String.format("Select %1$s.age, %1$s.name from db.%1$s where %1$s.salary >= %2$f and %1$s.age = 35", tableName, default_salary);
        int count = TestUtils.runSelect(manager, sqlParser, queryCountFirst, frequency);
        assertEquals(TEST_SIZE, count);

        String queryCountSecond = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.salary >= %2$s",
                tableName, default_salary + TEST_SIZE / 2);
        count = TestUtils.runSelect(manager, sqlParser, queryCountSecond, frequency);
        assertEquals(TEST_SIZE / 2, count);

    }

    @Test
    public void insertCompareTestDifferentRecords() {
        final int insertCount = 10_000; // different records

        String tableName = "testTable";
        Column ageColumn = new Column("Age", new Type(BaseType.INT));
        Column nameColumn = new Column("Name", Type.createType("varchar", 20));
        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        columns.add(nameColumn);
        manager.createTable(tableName, columns);

        SQLParser sqlParser = new SQLParser(manager);
        String query = "Insert into db." + tableName + " (name, age) values (\"Petr\", 0)";
        Statement statement = TestUtils.createStatement(sqlParser, query);

        final int step = 10;

        for (int i = 0; i < insertCount; i++) {
            ((Conditions) statement.getParam("conditions")).getValues().get(1).setValue((Comparable<Object>) (Object) (i * step));
            manager.insert(statement.getStringParam("table_name"), (Conditions) statement.getParam("conditions"));
        }

        final int range = 10;
        for (int i = 0; i < insertCount / 100; i++) {
            query = String.format("Select %1$s.age, %1$s.name from db.%1$s " +
                    "where age > %2$d and age <= %3$d", tableName, i * 10, i * 10 + range * step);
            int count = TestUtils.runSelect(manager, sqlParser, query, null);
            if (count != range) {
                System.out.println(manager.getTable(tableName).getIndex().toString());
                break;
            }
            assertEquals(range, count);
        }

    }

    @Test(expected = Exception.class)
    public void insertWrongTableTest() {
        String tableName = "testTable";
        manager.insert(tableName, new Conditions());
    }

    @Test
    public void deleteTest() {
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);
        String query = "Insert into db." + tableName + " (name, age, salary) values (\"Petr\", 22, 23504.5)";
        TestUtils.runInsert(manager, sqlParser, query);
        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.age = 22", tableName);
        int count = TestUtils.runSelect(manager, sqlParser, query, 1);
        assertEquals(1, count);

        query = "Delete from " + tableName + " where age = 21 and salary < 25000";
        int deletedCount = TestUtils.runDelete(manager, sqlParser, query);
        assertEquals(0, deletedCount);

        query = "Delete from " + tableName + " where age = 22 and salary < 25000";
        deletedCount = TestUtils.runDelete(manager, sqlParser, query);
        assertEquals(1, deletedCount);
    }

    @Test
    public void deleteTestBigDifferentRecords() {
        final int TEST_SIZE = 10_000;
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);

        double default_salary = 10000;
        for (int i = 0; i < TEST_SIZE; ++i) {
            String query = "Insert into db." + tableName + String.format(" (name, age, salary) values (\"Petr\", 22, %1$f)", default_salary + i);
            TestUtils.runInsert(manager, sqlParser, query);
        }

        String query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.age = 22", tableName);
        int count = TestUtils.runSelect(manager, sqlParser, query, 1000);
        assertEquals(TEST_SIZE, count);

        query = String.format("Delete from %1$s where age = 22 and salary < %2$f", tableName, default_salary + TEST_SIZE / 2);
        int deletedCount = TestUtils.runDelete(manager, sqlParser, query);
        assertEquals(TEST_SIZE / 2, deletedCount);

        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.age = 22 and %1$s.salary >= %2$f", tableName, default_salary + TEST_SIZE / 2);
        count = TestUtils.runSelect(manager, sqlParser, query, 1000);
        assertEquals(TEST_SIZE / 2, count);

        query = String.format("Delete from %1$s where age = 22 and salary > %2$f", tableName, default_salary + TEST_SIZE / 2);
        deletedCount = TestUtils.runDelete(manager, sqlParser, query);
        assertEquals(TEST_SIZE / 2 - 1, deletedCount);

        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where %1$s.age = 22", tableName);
        count = TestUtils.runSelect(manager, sqlParser, query, 1000);
        assertEquals(1, count);
    }

    @Test
    public void updateTest() {
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);

        String query = "Insert into db." + tableName + String.format(" (name, age, salary) values (\"Petr\", 22, %1$d)", 10000);
        TestUtils.runInsert(manager, sqlParser, query);

        query = "Insert into db." + tableName + String.format(" (name, age, salary) values (\"Anna\", 23, %1$d)", 15000);
        TestUtils.runInsert(manager, sqlParser, query);

        query = "Update " + tableName + " set age = 23, salary = 14000 where age = 22 and name = \"Petr\"";
        int count = TestUtils.runUpdate(manager, sqlParser, query);
        assertEquals(1, count);

        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where age > 22", tableName);
        count = TestUtils.runSelect(manager, sqlParser, query, 1);
        assertEquals(2, count);
    }

    @Test
    public void updateBigTest() {
        final int TEST_SIZE = 10_000;
        String tableName = "testTable";
        createTableRightColumnsTest();
        Table table = manager.getTable(tableName);
        assertNotNull(table);
        SQLParser sqlParser = new SQLParser(manager);

        double default_salary = 10000;
        for (int i = 0; i < TEST_SIZE; ++i) {
            String query = "Insert into db." + tableName + String.format(" (name, age, salary) values (\"Petr\", 22, %1$f)", default_salary + i * 10);
            TestUtils.runInsert(manager, sqlParser, query);

            query = "Insert into db." + tableName + String.format(" (name, age, salary) values (\"Anna\", 24, %1$f)", default_salary + i * 20);
            TestUtils.runInsert(manager, sqlParser, query);
        }

        String query = "Update " + tableName + " set age = 23, salary = 14000 where age = 22 and name = \"Petr\"";
        int count = TestUtils.runUpdate(manager, sqlParser, query);
        assertEquals(TEST_SIZE, count);

        query = String.format("Select %1$s.age, %1$s.name, %1$s.salary from db.%1$s where age > 22", tableName);
        count = TestUtils.runSelect(manager, sqlParser, query, 100);
        assertEquals(TEST_SIZE * 2, count);
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