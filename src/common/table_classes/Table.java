package common.table_classes;

import commands_runner.indexes.AbstractIndex;
import common.Column;
import common.Type;
import common.conditions.Condition;
import common.oracles.TableStatistics;
import common.utils.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;

import java.nio.file.Path;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public class Table {
    String name;
    String fileName;

    List<Column> columns;
    static final int MAX_COLUMN_COUNT = 10;
    static final int HEADER_SIZE = MAX_COLUMN_COUNT * Column.DECLARATION_BYTE_SIZE;
    int recordSize;

    TableStatistics tableStatistics = new TableStatistics();
    AbstractIndex index = null;

    public Table(String name, String fileName, List<Column> columns){
        this.name = name;
        this.columns = columns;
        this.fileName = fileName;
        recordSize = 0;
        calcRecordSize();
    }

    public Column getColumn(final String columnName) {
        for (Column column : columns)
            if (column.getName().equals(columnName.toUpperCase()))
                return column;
        return null;
    }

    public int getColumnIndex(Column column) {
        for (int i = 0; i < columns.size(); i++)
            if (columns.get(i).getName().equals(column.getName()))
                return i;
        return -1;
    }

    public int getColumnIndex(final String columnName) {
        for (int i = 0; i < columns.size(); i++)
            if (columns.get(i).getName().equals(columnName.toUpperCase()))
                return i;
        return -1;
    }

    public void setRecordSize(int recordSize) {
        this.recordSize = recordSize;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public void calcRecordSize() {
        recordSize = 0;
        if (columns != null)
            for (Column column : columns) {
                recordSize += column.getType().getSize();
                recordSize += Utils.getIntByteSize(); //TypeID fix
            }
    }

    public int calcMaxRecordCount() {
        return Page.calcMaxRecordCount(recordSize);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getFileName() {
        return fileName;
    }

    public String getIndexFileName() {
        int pos = fileName.lastIndexOf(".");
        String result = fileName;
        if (pos > 0) {
            result = fileName.substring(0, pos);
        }
        return result + ".idx";
    }

    public String getName() {
        return name;
   }

    public AbstractIndex getIndex() {
        return index;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setIndex(AbstractIndex index) {
        this.index = index;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
