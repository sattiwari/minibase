package minibase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by stiwari on 11/23/2016 AD.
 */
public class CatalogTest {
    private String name = "test";
    private String nameThisTestRun;

    @Before
    public void addTables() throws Exception {
        nameThisTestRun = UUID.randomUUID().toString();
        Database.getCatalog().addTable(new TestUtil.SkeltonFile(-1, Utility.getTupleDesc(2)), nameThisTestRun);
        Database.getCatalog().addTable(new TestUtil.SkeltonFile(-2, Utility.getTupleDesc(2)), name);
    }

    @Test
    public void getTupleDesc() throws Exception {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(-1);

        assertEquals(expected, actual);
    }

    @Test
    public void getTableId() {
        assertEquals(-2, Database.getCatalog().getTableId(name));
        assertEquals(-1, Database.getCatalog().getTableId(nameThisTestRun));

        try {
            Database.getCatalog().getTableId(null);
            Assert.fail("Should not find table with null name");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }

        try {
            Database.getCatalog().getTableId("foo");
            Assert.fail("Should not find table with name foo");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
    }

    @Test
    public void getDatabaseFile() throws Exception {
        DbFile f = Database.getCatalog().getDatabaseFile(-1);

        assertEquals(-1, f.getId());
    }

}