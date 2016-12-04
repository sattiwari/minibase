package minibase;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends Operator {

    private DbIterator child;
    private int aField;
    private int gField;
    private Aggregator.Op aop;
    private Aggregator aggregator;

    /**
     * Constructor.
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     *
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.aField = afield;
        this.gField = gfield;
        this.aop = aop;

        Type gFieldType = null;
        if (gfield != -1) {
            gFieldType = child.getTupleDesc().getFieldType(gfield);
        }
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, gFieldType, afield, aop);
        } else {
            this.aggregator = new StringAggregator(gfield, gFieldType, afield, aop);
        }

        try {
            child.open();
            while (child.hasNext()) {
                this.aggregator.mergeTupleIntoGroup(child.next());
            }
            child.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.child = this.aggregator.iterator();
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        switch (aop) {
            case MIN:
                return "min";
            case MAX:
                return "max";
            case AVG:
                return "avg";
            case SUM:
                return "sum";
            case COUNT:
                return "count";
        }
        return "";
    }

    public void open()
            throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (child.hasNext()) {
            return child.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     *
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void close() {
        child.close();
    }
}