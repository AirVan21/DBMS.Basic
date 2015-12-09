package buffer_manager;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by semionn on 09.10.15.
 */
public abstract class AbstractBufferManager implements IBufferManager {
    // Up to 256 Pages
    int maxPagesCount = 256;
    public static final Path DATA_ROOT_DB_FILE = Paths.get("data//root_db.ndb");
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
