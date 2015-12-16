package buffer_manager;

import common.BaseType;
import common.Column;
import common.NullObject;
import common.Type;
import common.conditions.ComparisonType;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.exceptions.ReadPageException;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;
import common.utils.Utils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by airvan21 on 03.12.15.
 */
public class LoadEngine {
    private Integer maxPagesCount;
    private int[] usedPages;
    private List<Page> pageBuffer;
    private RandomAccessFile tableFile;
    private Table table;

    // pageIndex -> buffer position
    private Map<Integer, Integer> pageIndBufferPos;
    private int firstFullPageIndex = 0;
    private int firstIncompletePageIndex = 0;
    private int bufferPosition = 0;

    public LoadEngine(Integer maxPages) {
        maxPagesCount = maxPages;
        usedPages = new int[maxPagesCount];
        pageIndBufferPos = new HashMap<>();
        pageBuffer = new ArrayList<>(maxPagesCount);
    }

    /*
        Switch to exists File for table "fileName" in ../data/
    */
    public void switchToTable(Table table) {
        if (this.table != table)
            try {
                this.table = table;
                if (tableFile != null)
                    tableFile.close();
                tableFile = new RandomAccessFile(table.getFileName(), "rw");
                // Get table context
                readMetaPage(table);
    //            if (pageBuffer.size() == 0)
    //                pageBuffer.add(new Page(table));
            } catch (FileNotFoundException e) {
                System.out.println("Problems in RandomAccessFile creation");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }


    /*
        Creates new File for table "fileName" in ../data/
    */
    public void switchToNewTable(Table table) {
        if (this.table != table)
            try {
                this.table = table;
                if (tableFile != null)
                    tableFile.close();
                tableFile = new RandomAccessFile(table.getFileName(), "rw");
                // Get table context
//                if (pageBuffer.size() == 0)
//                    pageBuffer.add(new Page(table));
            } catch (FileNotFoundException e) {
                System.out.println("Problems in RandomAccessFile creation");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    /*
        Creates serializable page with meta-info and writs int to tableFile

        Default meta-header layout
        int : recordSize()
        int : amount of columns
        ---- for amount of columns ----
        int : column type length
        int : column name length
        char[length] : column name
        ----          end          ----
        int : amount of full pages
        int : first incomplete page

    */
    public void writeMetaPage(Table table) {
        try {
            tableFile.setLength(Page.PAGE_SIZE);

            final long startByte = 0;
            tableFile.seek(startByte);
            // Write record size
            tableFile.writeInt(table.getRecordSize());
            List<Column> columns = table.getColumns();
            // Amount of columns
            tableFile.writeInt(columns.size());
            for (Column column : columns) {
                tableFile.writeInt(column.getType().getBaseType().getTypeNumber());
                // 2 - byte char
                tableFile.writeInt(column.getName().length() * Utils.getCharByteSize());
                tableFile.writeChars(column.getName());
            }
            tableFile.writeInt(firstFullPageIndex);
            tableFile.writeInt(firstIncompletePageIndex);
            tableFile.getFD().sync();
        } catch (FileNotFoundException e) {
            System.out.print("Couldn't write table file!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readMetaPage(Table table) {
        try {
            final long startByte = 0;
            tableFile.seek(startByte);
            table.setRecordSize(tableFile.readInt());
            int amountOfColumns = tableFile.readInt();
            List<Column> columns = new ArrayList<>();
            for (int i = 0; i < amountOfColumns; ++i) {
                int typeID = tableFile.readInt();
                Type type = new Type(BaseType.createBaseType(typeID));
                int columnNameLength = tableFile.readInt();
                byte[] columnName = new byte[columnNameLength];
                tableFile.read(columnName, 0, columnNameLength);
                columns.add(new Column(new String(columnName, "UTF-16").trim(), type));
            }
            table.setColumns(columns);
            firstFullPageIndex = tableFile.readInt();
            firstIncompletePageIndex = tableFile.readInt();
            table.calcRecordSize();
        } catch (FileNotFoundException e) {
            System.out.print("Couldn't read table file!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // return buffer position for loaded page
    public int loadPageInBuffer(int pageIndex) throws ReadPageException {
        for (int i = 0; i < pageBuffer.size(); ++i) {
            if (pageBuffer.get(i).pageId == pageIndex + 1 && table.getName().equals(pageBuffer.get(i).table.getName())) {
                return i;
            }
        }
        // Not in buffer
        int bufferPos = nextBufferPos();
        if (bufferPos >= 0) {
            Page pageToFill = pageBuffer.get(bufferPos);
            pageToFill.pageId = pageIndex + 1;
            pageToFill.table = table;
            if (checkPageInFile(pageToFill.pageId)) {
                loadPageFromFile(pageToFill);
                pageBuffer.add(bufferPos, pageToFill);
            }
            return bufferPos;
        }
        // No page
        throw new ReadPageException(String.format("No page with index %s", pageIndex));
    }

    private boolean checkPageInFile(int pageID) {
        try {
            return (pageID + 1) * Page.PAGE_SIZE <= tableFile.length();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void storeRecordInPage(Record record) {
        try {
//            if (pageBuffer.size() < firstIncompletePageIndex)
//                storePageInFile(firstIncompletePageIndex);
            int index = loadPageInBuffer(firstIncompletePageIndex);
            Page pageToAdd = pageBuffer.get(index);
            if (pageToAdd.isFull()) {
                pageToAdd = new Page(table);
                int last_pos = bufferPosition;
                int nextPos = nextBufferPos();
                firstIncompletePageIndex += 1;
                pageToAdd.pageId = firstIncompletePageIndex + 1;
                try {
                    tableFile.setLength(tableFile.length() + Page.PAGE_SIZE);
                    storePageInFile(index);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (pageBuffer.size() == maxPagesCount)
                    pageBuffer.remove(last_pos);
                pageBuffer.add(nextPos, pageToAdd);
            }
            pageToAdd.addRecord(record);
        } catch (ReadPageException e) {
            e.printStackTrace();
        }
    }

    /*
        Find in hashmap index of page or load page if it's not in buffer
     */
    public Page getPageFromBuffer(int pageID) throws ReadPageException {
        return pageBuffer.get(loadPageInBuffer(pageID - 1));
    }

    public void loadPageFromFile(Page fillPage) {
        try {
            tableFile.seek(fillPage.pageId * Page.PAGE_SIZE);
            tableFile.readInt();
            fillPage.deleted = tableFile.readBoolean();
            fillPage.dirty = tableFile.readBoolean();
            fillPage.full = tableFile.readBoolean();
            int recordCount = tableFile.readInt();
            for (int i = 0; i < recordCount; ++i) {
                Conditions assignment = new Conditions();
                for (int j = 0; j < table.getColumns().size(); ++j) {
                    int typeID = tableFile.readInt();
                    BaseType type = BaseType.createBaseType(typeID);
                    Condition condition = null;
                    switch (type) {
                        case VARCHAR:
                            byte[] string = new byte[Type.MAX_STRING_BYTE_SIZE];
                            tableFile.read(string, 0, Type.MAX_STRING_BYTE_SIZE);
                            String stringField = new String(string, "UTF-16").trim();
                            condition = new Condition(table, table.getColumns().get(j), ComparisonType.EQUAL, stringField);
                            break;
                        case DOUBLE:
                            double dValue = tableFile.readDouble();
                            condition = new Condition(table, table.getColumns().get(j), ComparisonType.EQUAL, dValue);
                            break;
                        case INT:
                            int iValue = tableFile.readInt();
                            condition = new Condition(table, table.getColumns().get(j), ComparisonType.EQUAL, iValue);
                            break;
                    }
                    assignment.addValue(condition);
                }
                Record record = new Record(table.getColumns(), assignment);
                fillPage.addRecord(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void storePageInFile(int pageIndex) {
        try {
            if (tableFile.length() / Page.PAGE_SIZE < pageIndex + 2) {
                tableFile.setLength(Page.PAGE_SIZE * (pageIndex + 2));
            }
            tableFile.seek((1 + pageIndex) * Page.PAGE_SIZE);
            Page pageToWrite;
            if (pageIndex >= pageBuffer.size()) {
                pageToWrite = new Page(table);
                pageToWrite.pageId = pageIndex + 1;
                pageBuffer.add(pageToWrite);

            } else {
                pageToWrite = pageBuffer.get(pageIndex);
            }
            tableFile.writeInt(pageToWrite.pageId);
            tableFile.writeBoolean(pageToWrite.deleted);
            tableFile.writeBoolean(pageToWrite.dirty);
            tableFile.writeBoolean(pageToWrite.full);
            tableFile.writeInt(pageToWrite.getRecordsCount());
            List<Column> columns = table.getColumns();
            for (Record record : pageToWrite.getAllRecords()) {
                for (int i = 0; i < table.getColumns().size(); i++) {
                    Object value = record.getColumnValue(i);
                    tableFile.writeInt(columns.get(i).getType().getBaseType().getTypeNumber());
                    switch (columns.get(i).getType().getBaseType()) {
                        case VARCHAR:
                            long pos = tableFile.getFilePointer();
                            tableFile.write(((String) value).getBytes("UTF-16"));
                            tableFile.seek(pos + Type.MAX_STRING_BYTE_SIZE);
                            break;
                        case DOUBLE:
                            tableFile.writeDouble((double) value);
                        case INT:
                            tableFile.writeInt((int) value);
                            break;
                    }
                }
            }
            tableFile.getFD().sync();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int nextBufferPos() {
//        try {
//            if (tableFile.length() == Page.PAGE_SIZE) {
//                boolean flag = false;
//                //TODO: VERY STUPID WAY
//                for (int i = 0; i < pageBuffer.size(); i++)
//                    if (pageBuffer.get(i).table == table) {
//                        flag = true;
//                        break;
//                    }
//                if (!flag)
//                    return -1;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        if (pageBuffer.size() == 0)
        {
            pageBuffer.add(new Page(table));
            return 0;
        }
        // perform algo
        bufferPosition = (bufferPosition + 1) % maxPagesCount;
        if (bufferPosition >= pageBuffer.size())
            pageBuffer.add(bufferPosition, new Page(table));
        return bufferPosition;
    }

    public long sizeInPages() {
        try {
            return tableFile.length() / Page.PAGE_SIZE;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private void searchFirstIncompletePage() {
        for (int i = 0; i < pageBuffer.size(); ++i) {
            if (!pageBuffer.get(i).isFull() && pageBuffer.get(i).table == table) {
                firstIncompletePageIndex = i;
                return;
            }
        }
    }

    public void flushTableData() {
        for (int i = 0; i < pageBuffer.size(); i++) {
            if (pageBuffer.get(i).table == table)
                storePageInFile(i);
        }
    }

    public void flushAllData() {
        Set<Table> allTables = new HashSet<>();
        for (Page page : pageBuffer) {
            allTables.add(page.table);
        }
        for (Table table : allTables) {
            switchToTable(table);
            flushTableData();
        }
    }

}
