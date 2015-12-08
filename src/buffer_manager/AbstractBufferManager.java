package buffer_manager;

import java.io.RandomAccessFile;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public abstract class AbstractBufferManager implements IBufferManager {
    // Up to 256 Pages
    int maxPagesCount = 256;
    // Buffer managers with different structure
    List<IBufferManager> bufferManagerList;

    public AbstractBufferManager(int maxPagesCount)
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
