package minibase;

import minibase.systemtest.SimpleDbTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterTest extends SimpleDbTestBase {

    int testWidth = 3;
    DbIterator scan;

    @Before public void setUp() {
        this.scan = new TestUtil.MockScan(-5, 5, testWidth);
    }

    @Test public void getTupleDesc() {
        Predicate pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(0));
        Filter op = new Filter(pred, scan);
        TupleDesc expected = Utility.getTupleDesc(testWidth);
        TupleDesc actual = op.getTupleDesc();
        assertEquals(expected, actual);
    }

    @Test public void rewind() throws Exception {
        Predicate pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(0));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(op.hasNext());
        assertNotNull(op.next());
        assertTrue(TestUtil.checkExhausted(op));

        op.rewind();
        Tuple expected = Utility.getHeapTuple(0, testWidth);
        Tuple actual = op.next();
        assertTrue(TestUtil.compareTuples(expected, actual));
        op.close();
    }

    @Test public void filterSomeLessThan() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Predicate.Op.LESS_THAN, TestUtil.getField(2));
        Filter op = new Filter(pred, scan);
        TestUtil.MockScan expectedOut = new TestUtil.MockScan(-5, 2, testWidth);
        op.open();
        TestUtil.compareDbIterators(op, expectedOut);
        op.close();
    }

    @Test public void filterAllLessThan() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Predicate.Op.LESS_THAN, TestUtil.getField(-5));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(TestUtil.checkExhausted(op));
        op.close();
    }

    @Test public void filterEqual() throws Exception {
        Predicate pred;
        this.scan = new TestUtil.MockScan(-5, 5, testWidth);
        pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(-5));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(TestUtil.compareTuples(Utility.getHeapTuple(-5, testWidth),
                op.next()));
        op.close();

        this.scan = new TestUtil.MockScan(-5, 5, testWidth);
        pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(0));
        op = new Filter(pred, scan);
        op.open();
        assertTrue(TestUtil.compareTuples(Utility.getHeapTuple(0, testWidth),
                op.next()));
        op.close();

        this.scan = new TestUtil.MockScan(-5, 5, testWidth);
        pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(4));
        op = new Filter(pred, scan);
        op.open();
        assertTrue(TestUtil.compareTuples(Utility.getHeapTuple(4, testWidth),
                op.next()));
        op.close();
    }

    @Test public void filterEqualNoTuples() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(5));
        Filter op = new Filter(pred, scan);
        op.open();
        TestUtil.checkExhausted(op);
        op.close();
    }
}