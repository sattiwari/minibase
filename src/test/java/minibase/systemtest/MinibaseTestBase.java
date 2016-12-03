package minibase.systemtest;

import org.junit.Before;

import minibase.Database;

/**
 * Base class for all minibase test classes.
 *
 * @author nizam
 *
 */
public class MinibaseTestBase {
    /**
     * Reset the database before each test is run.
     */
    @Before
    public void setUp() throws Exception {
        Database.reset();
    }

}