package common.table_classes;

import commands_runner.indexes.AbstractIndex;
import common.Column;
import common.Type;
import common.conditions.Condition;
import common.oracles.TableStatistics;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;

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

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public Table(String name, String fileName, List<Column> columns){
        this.name = name;
        this.columns = columns;
        this.fileName = fileName;
        recordSize = 0;
        if (columns != null)
            for (Column column : columns) {
                recordSize += column.getType().getSize();
            }
    }

    public Column getColumn(final String columnName) {
        return CollectionUtils.find(columns, new Predicate() {
            public boolean evaluate(Object a) {
                return ((Column) a).getName().equals(columnName.toUpperCase());
            }
        });
    }

    public int getColumnIndex(Column column) {
        return columns.indexOf(column);
    }

    public void setRecordSize(int recordSize) {
        this.recordSize = recordSize;
    }

    public int getRecordSize() {
        return recordSize;

    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getFileName() {
        return fileName;
    }

    public String getName() {
        return name;
    }

    public AbstractIndex getIndex() {
        return index;
    }
}
