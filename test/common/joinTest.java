package common;

import commands_runner.TableManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Created by airvan21 on 27.01.16.
 */
public class joinTest {

    private TableManager manager = null;
    private final String dbFolder = "dataTest//";

    @Before
    public void beforeTest() {
        TestUtils.deleteDirectory(new File(dbFolder));
        Integer bufferPoolSize = 16;
        manager = new TableManager(bufferPoolSize, dbFolder);
    }

    @Test
    public void simpleJoinTest() {

    }

    @After
    public void afterTest() {
        try {
            manager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
