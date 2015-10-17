package buffer_manager;

import common.Condition;
import common.table_classes.Page;
import common.table_classes.Table;

import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public abstract class AbstractBufferManager implements IBufferManager {
    final Integer PAGE_SIZE = 4 * 1024;
    final Integer maxPagesCount;
    List<IBufferManager> bufferManagerList;
    Table table;

    public AbstractBufferManager(Integer maxPagesCount)
    {
        this.maxPagesCount = maxPagesCount;
    }

    Integer getMaxPageCount() { return maxPagesCount; }

    /* TODO:
    Add functions:
        Load page
        Load page metadata

        Page searching - load metadata from page and use next number to iterate = O(N)
     */


}
