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
    public static final String DATA_ROOT_DB_NAME = "root_db.ndb";
    public static Path DATA_ROOT_DB_FILE;

    // Buffer managers with different structure
    List<IBufferManager> bufferManagerList;

    public AbstractBufferManager(int maxPagesCount)
    {
        this.maxPagesCount = maxPagesCount;
    }

    Integer getMaxPageCount() { return maxPagesCount; }

}
