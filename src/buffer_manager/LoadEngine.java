package buffer_manager;

import commands_runner.indexes.btree.BTreeSerializer;
import common.BaseType;
import common.Column;
import common.Type;
import common.conditions.ComparisonType;
import common.conditions.Condition;
import common.conditions.Conditions;
import common.exceptions.ReadPageException;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;
import common.utils.Utils;

import javax.naming.OperationNotSupportedException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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

    private int firstFullPageIndex = 0;
    private int firstIncompletePageIndex = 0;
    private int bufferPosition = 0;

    public LoadEngine(Integer maxPages) {
        maxPagesCount = maxPages;
        usedPages = new int[maxPagesCount];
        pageBuffer = new ArrayList<>(maxPagesCount);
    }

    /*
        Switch to exists File for table "fileName" in ../data/
    */
    public void switchToTable(Table table) {
        if (this.table != table)
            try {
                this.table = table;
                if (tableFile != null) {
                    tableFile.getFD().sync();
                    tableFile.close();
                }
                tableFile = new RandomAccessFile(table.getFileName(), "rw");
                // Get table context
                readMetaPage(table);
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
            tableFile.close();
            tableFile = new RandomAccessFile(table.getFileName(), "rw");
            if (tableFile.length() < Page.PAGE_SIZE) {
                tableFile.setLength(Page.PAGE_SIZE);
            }

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
    public int loadTablePageInBuffer(int pageIndex) throws ReadPageException {
        for (int i = 0; i < pageBuffer.size(); ++i) {
            if (pageBuffer.get(i).pageId == pageIndex + 1 &&
                    table.getName().equals(pageBuffer.get(i).table.getName()) &&
                    !pageBuffer.get(i).isIndex()) {
                return i;
            }
        }
        // Not in buffer
        int bufferPos = nextBufferPos(true);
        if (bufferPos >= 0) {
            Page pageToFill = pageBuffer.get(bufferPos);
            if (pageToFill.isIndex()) {
                if (pageToFill.dirty)
                    storeIndexPageInFile(bufferPos, table.getIndex().getKeyType());
                pageBuffer.set(bufferPos, new Page(table));
                pageToFill = pageBuffer.get(bufferPos);
            } else if (pageToFill.dirty)
                storePageInFile(bufferPos);

            pageToFill.pageId = pageIndex + 1;
            pageToFill.table = table;
            pageToFill.refreshPage();
            if (checkPageInFile(pageToFill.pageId)) {
                loadPageFromFile(pageToFill);
            }
            return bufferPos;
        }
        // No page
        throw new ReadPageException(String.format("No page with index %s", pageIndex));
    }

    public int loadTreeIndexPageInBuffer(int pageID, int order, Type keyType) throws ReadPageException {
        int bufferPos = findIndexPage(pageID);
        if (bufferPos != -1)
            return bufferPos;
        try {
            RandomAccessFile file = new RandomAccessFile(table.getIndexFileName(), "rw");
            int pos = BTreeSerializer.readNodePage(pageID, table, order, this, file, keyType, true);
            file.close();
            return pos;
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new ReadPageException(String.format("No page with index %s", pageID));
    }

    public int loadIndexPageInBuffer(Page page, int order, Type keyType) {
        int bufferPos = findIndexPage(page.pageId);
        if (bufferPos == -1) {
            bufferPos = nextBufferPos(true);
            if (pageBuffer.get(bufferPos).dirty)
                storePage(bufferPos, order, keyType);
        }
        pageBuffer.set(bufferPos, page);
        return bufferPos;
    }

    public int findIndexPage(int pageID) {
        for (int i = 0; i < pageBuffer.size(); ++i) {
            if (pageBuffer.get(i).pageId == pageID &&
                    table.getName().equals(pageBuffer.get(i).table.getName()) &&
                    pageBuffer.get(i).isIndex()) {
                return i;
            }
        }
        return -1;
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
//            if (pageBuffer.getSize() < firstIncompletePageIndex)
//                storePageInFile(firstIncompletePageIndex);
            int index = loadTablePageInBuffer(firstIncompletePageIndex);
            Page pageToAdd = pageBuffer.get(index);
            if (pageToAdd.isFull()) {
                pageToAdd = new Page(table);
                int nextPos = nextBufferPos(false);
                firstIncompletePageIndex += 1;
                pageToAdd.pageId = firstIncompletePageIndex + 1;
                storePageInFile(index);
                if (pageBuffer.size() == maxPagesCount)
                    pageBuffer.set(nextPos, pageToAdd);
                else
                    pageBuffer.add(nextPos, pageToAdd);
            }
            pageToAdd.addRecord(record);
        } catch (ReadPageException e) {
            e.printStackTrace();
        }
    }

    /*
        Find in index of page or load page if it's not in buffer
     */
    public Page getPageFromBuffer(int pageID){
        try {
            return pageBuffer.get(loadTablePageInBuffer(pageID - 1));
        } catch (ReadPageException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Page getTreeIndexPageFromBuffer(int pageID, int order, Type keyType) {
        try {
            return pageBuffer.get(loadTreeIndexPageInBuffer(pageID, order, keyType));
        } catch (ReadPageException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void loadPageFromFile(Page fillPage) {
        try {
            tableFile.seek(fillPage.pageId * Page.PAGE_SIZE);
            int id = tableFile.readInt();
            fillPage.deleted = tableFile.readBoolean();
            tableFile.readBoolean();
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

            fillPage.dirty = false;
            // deleteMask
            int bytesInDeleteMask = tableFile.readInt();
            byte[] deleteMaskBytes = new byte[bytesInDeleteMask];
            tableFile.read(deleteMaskBytes, 0, bytesInDeleteMask);
            fillPage.deletedMask = Utils.toBitSet(deleteMaskBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void storePage(int bufferPos, int order, Type keyType) {
        Page page = pageBuffer.get(bufferPos);
        if (page.isIndex())
            storeIndexPageInFile(page.pageId, order, keyType);
        else
            storePageInFile(bufferPos);
    }

    public void storePageInFile(int bufferPos) {
        try {
            if (bufferPos >= pageBuffer.size())
                return;
            Page pageToWrite = pageBuffer.get(bufferPos);

            if (tableFile.length() / Page.PAGE_SIZE < pageToWrite.pageId + 1) {
                tableFile.setLength(Page.PAGE_SIZE * (pageToWrite.pageId + 1));
            }

            pageToWrite.dirty = false;

            tableFile.seek(pageToWrite.pageId * Page.PAGE_SIZE);
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
                            break;
                        case INT:
                            tableFile.writeInt((int) value);
                            break;
                    }
                }
            }

            byte[] deleteMaskBytes = Utils.toByteArray(pageToWrite.deletedMask);
            tableFile.writeInt(deleteMaskBytes.length);
            tableFile.write(deleteMaskBytes);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void storeIndexPageInFile(int pageID, int order, Type keyType) {
//        try {
            int bufferPos = findIndexPage(pageID);
            if (bufferPos == -1)
                return;
            storeIndexPageInFile(bufferPos, keyType);
//        } catch (ReadPageException e) {
//            e.printStackTrace();
//        }
    }

    public void storeIndexPageInFile(int bufferPos, Type keyType) {
        try {
            RandomAccessFile file = new RandomAccessFile(table.getIndexFileName(), "rw");
            BTreeSerializer.writeNodePage(pageBuffer.get(bufferPos), this, file, keyType);
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int nextBufferPos(boolean add) {
        if (pageBuffer.size() == 0) {
            if (add)
                pageBuffer.add(new Page(table));
            return 0;
        }
        // perform algo
        bufferPosition = (bufferPosition + 1) % maxPagesCount;
        if (bufferPosition >= pageBuffer.size())
            if (add && pageBuffer.size() < maxPagesCount)
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

    public boolean isRecordDeleted(int offset) {
        int pageID = offset / Page.PAGE_SIZE;
        int recordPos = (offset % Page.PAGE_SIZE - Page.HEADER_SIZE) / table.getRecordSize();
        Page page = getPageFromBuffer(pageID);
        return page.deletedMask.get(recordPos);
    }

    public Record getRecordByOffset(int offset) {
        int pageID = offset / Page.PAGE_SIZE;
        int recordPos = (offset % Page.PAGE_SIZE - Page.HEADER_SIZE) / table.getRecordSize();
        Page page = getPageFromBuffer(pageID);
        if (page == null)
            return null;
        return page.getRecord(recordPos);
    }

    public int calcRecordOffset(int pageID, int recordNum) {
        return Page.PAGE_SIZE * pageID + Page.HEADER_SIZE + recordNum * table.getRecordSize();
    }

    public void flushTableData() {
        for (int i = 0; i < pageBuffer.size(); i++) {
            if (pageBuffer.get(i).table == table && pageBuffer.get(i).dirty)
                if (pageBuffer.get(i).isIndex())
                    storeIndexPageInFile(i, table.getIndex().getKeyType());
                else
                    storePageInFile(i);
        }
    }

    public void flushIndexTableData() {
        for (int i = 0; i < pageBuffer.size(); i++) {
            if (pageBuffer.get(i).table == table)
                if (pageBuffer.get(i).isIndex())
                    storeIndexPageInFile(i, table.getIndex().getKeyType());
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
            writeMetaPage(table);
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Page page : pageBuffer) {
            stringBuilder.append(String.format("%d %b", page.pageId, page.isIndex()));
        }
        return stringBuilder.toString();
    }

}
