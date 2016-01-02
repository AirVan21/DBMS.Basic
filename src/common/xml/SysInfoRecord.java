package common.xml;

/**
 * Created by semionn on 01.01.16.
 */
public class SysInfoRecord {
    public String tableName;
    public String tablePath;
    public String indexType;
    public String indexPath;

    public SysInfoRecord(String tableName, String path, String indexType, String indexPath) {
        this.indexPath = indexPath;
        this.indexType = indexType;
        this.tablePath = path;
        this.tableName = tableName;
    }
}
