package buffer_manager;

import common.Column;
import common.table_classes.Page;
import common.table_classes.Table;
import common.utils.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by airvan21 on 03.12.15.
 */
public class LoadEngine {
    private Integer maxPagesCount;
    private int[] usedPages;
    private ByteBuffer buffer;
    private RandomAccessFile tableFile;

    // pageIndex -> buffer position
    private Map<Integer, Integer> pageIndBufferPos;
    private int firstFullPageIndex = 0;
    private int firstIncompletePageIndex = 1; // Skip Meta-Page

    public LoadEngine(Integer maxPages) {
        maxPagesCount = maxPages;
        usedPages = new int[maxPagesCount];
        pageIndBufferPos = new HashMap<>();
        buffer = ByteBuffer.allocate(Page.PAGE_SIZE * maxPagesCount);
    }

    /*
        Creates new File for table "fileName" in ../data/
    */
    public void switchToTable(String filePath) {
        try {
            tableFile = new RandomAccessFile(filePath, "rw");
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
                tableFile.writeInt(column.getType().getSize());
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
            int recordSize = tableFile.readInt();
            int amountOfColumns = tableFile.readInt();
            for (int i = 0; i < amountOfColumns; ++i) {
                int typeLength = tableFile.readInt();
                int columnNameLength = tableFile.readInt();
                tableFile.skipBytes(columnNameLength);
            }
            firstFullPageIndex = tableFile.readInt();
            firstFullPageIndex = tableFile.readInt();
            tableFile.close();
        } catch (FileNotFoundException e) {
            System.out.print("Couldn't read table file!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
