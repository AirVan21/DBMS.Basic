import commands_runner.TableManager;
import common.Column;
import common.Type;
import common.BaseType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by semionn on 06.10.15.
 */

public class console {

    public static void main( final String[] args ){
        System.out.println("Hello DBMS!");
        String dbFolder = "data//";
        Integer bufferPoolSize = 512;
        Type type = new Type(BaseType.INT);
        Column ageColumn = new Column("Age", type);
        List<Column> columns = new ArrayList<>();
        columns.add(ageColumn);
        TableManager defaultManager = new TableManager(bufferPoolSize, dbFolder);
        defaultManager.createTable("person", columns);
    }
}