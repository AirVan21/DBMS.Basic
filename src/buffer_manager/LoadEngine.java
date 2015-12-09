package buffer_manager;

import common.BaseType;
import common.Column;
import common.Type;
import common.table_classes.Page;
import common.table_classes.Record;
import common.table_classes.Table;
import common.utils.Utils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private int firstIncompletePageIndex = 1; // Skip Meta-Page
    private int bufferPosition = 0;

    public LoadEngine(Integer maxPages) {
        maxPagesCount = maxPages;
        usedPages = new int[maxPagesCount];
        pageIndBufferPos = new HashMap<>();
        pageBuffer = new ArrayList<>(maxPagesCount);
    }

    /*
        Creates new File for table "fileName" in ../data/
    */
    public void switchToTable(Table table) {
        try {
            this.table = table;
            tableFile = new RandomAccessFile(table.getFileName(), "rw");
        } catch (FileNotFoundException e) {
            System.out.println("Problems in RandomAccessFile creation");
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
            tableFile.close();
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
            // Write record size
            table.setRecordSize(tableFile.readInt());
            int amountOfColumns = tableFile.readInt();
            List<Column> columns = new ArrayList<>();
            for (int i = 0; i < amountOfColumns; ++i) {
                int typeID = tableFile.readInt();
                Type type = new Type(BaseType.createBaseType(typeID));
                int columnNameLength = tableFile.readInt();
                byte[] columnName = new byte[columnNameLength];
                tableFile.read(columnName, 0, columnNameLength);
                columns.add(new Column(new String(columnName, "UTF-8"), type));
            }
            table.setColumns(columns);
            firstFullPageIndex = tableFile.readInt();
            firstFullPageIndex = tableFile.readInt();
            tableFile.close();
        } catch (FileNotFoundException e) {
            System.out.print("Couldn't read table file!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int loadPageInBuffer(int pageIndex)
    {
        try {
            // Check if already in Buffer
            // if not:
            int bufferPos = nextBufferPos();
            tableFile.seek(pageIndex * Page.PAGE_SIZE);
            Page pageToFill = pageBuffer.get(bufferPos);
            // TODO: parse bytes to Page
            throw new NotImplementedException();
            // return  bufferPos;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void storeRecordInPage(Record record)
    {
        int index = loadPageInBuffer(firstIncompletePageIndex);
        Page pageToAdd = pageBuffer.get(index);
        if (pageToAdd.isFull()) {
            pageToAdd = new Page(table.getRecordSize());
            int replacePos = nextBufferPos();
            try {
                tableFile.setLength(tableFile.length() + Page.PAGE_SIZE);
                storePageInFile(index);
            } catch (IOException e) {
                e.printStackTrace();
            }
            pageBuffer.remove(replacePos);
            pageBuffer.add(replacePos, pageToAdd);
        }
        pageToAdd.addRecord(record);
    }

    /*
        Find in hashmap index of page or load page if it's not in buffer
     */
    public Page getPageFromBuffer(int pagIndex){
        throw new NotImplementedException();
    }

    public void storePageInFile(int pageIndex)
    {
        try {
            tableFile.seek(pageIndex * Page.PAGE_SIZE);
            Page pageToWrite = pageBuffer.get(pageIndex);
            tableFile.writeBoolean(pageToWrite.deleted);
            tableFile.writeBoolean(pageToWrite.dirty);
            tableFile.writeBoolean(pageToWrite.full);
            List<Column> columns = table.getColumns();
            for (Record record : pageToWrite.getAllRecords())
            {
                for (int i = 0; i < table.getColumns().size(); i++) {
                    Object value = record.getColumnValue(i);
                    switch (columns.get(i).getType().getBaseType()) {
                        case VARCHAR:
                            tableFile.writeChars((String)value);
                            break;
                        case DOUBLE:
                            tableFile.writeDouble((double)value);
                        case INT:
                            tableFile.writeInt((int)value);
                            break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int nextBufferPos()
    {
        // perform algo
        bufferPosition = (bufferPosition + 1) % maxPagesCount;
        return bufferPosition;
    }

    public long sizeInPages()
    {
        try {
            return tableFile.length() / Page.PAGE_SIZE;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
