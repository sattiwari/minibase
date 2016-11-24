package minibase;

import org.junit.Before;
import org.junit.Test;
import minibase.TestUtil;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by stiwari on 11/23/2016 AD.
 */
public class CatalogTest {
    private String nameThisRun;

    @Before
    public void addTables() throws Exception {
        nameThisRun = UUID.randomUUID().toString();
        Database.getCatalog().addTable(new TestUtil.SkeltonFile(-1, Utility.getTupleDesc(2)), nameThisRun);
    }

    @Test
    public void getTupleDesc() throws Exception {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(-1);

        assertEquals(expected, actual);
    }

}