package simpledb.execution;

import java.util.*;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int numGbfield;
    private Type gbfieldtype;
    private int numAfield;
    private Op what;

    private HashMap<Field, Integer> aggTuples;
    private HashMap<Field, Integer> counters;
    TupleDesc td;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.numGbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.numAfield = afield;
        this.what = what;
        this.aggTuples = new HashMap<Field, Integer>();
        if (this.what == Op.AVG)
            this.counters = new HashMap<Field, Integer>();

        if (numGbfield != NO_GROUPING)
            this.td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE },
                    new String[] { "groupField", "aggregateValue" });
        else
            this.td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggregateValue" });
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbfield;
        try {
            gbfield = tup.getField(numGbfield);
        } catch (NoSuchElementException e) {
            gbfield = null;
        }
        int aggvalue = ((IntField) tup.getField(numAfield)).getValue();
        switch (what) {
            case MIN: {
                if (!aggTuples.containsKey(gbfield))
                    aggTuples.put(gbfield, aggvalue);
                else
                    aggTuples.put(gbfield, Integer.min(aggTuples.get(gbfield), aggvalue));
                break;
            }
            case MAX: {
                if (!aggTuples.containsKey(gbfield))
                    aggTuples.put(gbfield, aggvalue);
                else
                    aggTuples.put(gbfield, Integer.max(aggTuples.get(gbfield), aggvalue));
                break;
            }
            case SUM: {
                if (!aggTuples.containsKey(gbfield))
                    aggTuples.put(gbfield, aggvalue);
                else
                    aggTuples.put(gbfield, aggTuples.get(gbfield) + aggvalue);
                break;
            }
            case COUNT: {
                if (!aggTuples.containsKey(gbfield))
                    aggTuples.put(gbfield, 1);
                else
                    aggTuples.put(gbfield, aggTuples.get(gbfield) + 1);
                break;
            }
            case AVG: {
                if (!aggTuples.containsKey(gbfield)) {
                    aggTuples.put(gbfield, aggvalue);
                    counters.put(gbfield, 1);
                } else {
                    aggTuples.put(gbfield, aggTuples.get(gbfield) + aggvalue);
                    counters.put(gbfield, counters.get(gbfield) + 1);
                }
                break;
            }
            case SC_AVG: {
                throw new UnsupportedOperationException("SC_AVG is not implemented");
            }
            case SUM_COUNT: {
                throw new UnsupportedOperationException("SUM_COUNT is not implemented");
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    private class AggregateIterator implements OpIterator {
        /**
         * Opens the iterator. This must be called before any of the other methods.
         * 
         * @throws DbException when there are problems opening/accessing the database.
         */
        private Iterator<Map.Entry<Field, Integer>> itr;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            itr = aggTuples.entrySet().iterator();
        }

        /**
         * Returns true if the iterator has more tuples.
         * 
         * @return true f the iterator has more tuples.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (itr == null)
                throw new IllegalStateException("IntegerAggregate Iterator is not opened");

            return itr.hasNext();
        }

        /**
         * Returns the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return the next tuple in the iteration.
         * @throws NoSuchElementException if there are no more tuples.
         * @throws IllegalStateException  If the iterator has not been opened
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (itr == null)
                throw new IllegalStateException("IntegerAggregator Iterator is not opened");

            if (!itr.hasNext())
                throw new NoSuchElementException("IntegerAggregator has no more tuples");

            Tuple tuple = new Tuple(td);
            Map.Entry<Field, Integer> entry = itr.next();
            int val = entry.getValue();
            if (what == Op.AVG) {
                val = val / counters.get(entry.getKey());
            }

            if (numGbfield != NO_GROUPING) {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(val));
            } else
                tuple.setField(0, new IntField(val));

            return tuple;
        }

        /**
         * Resets the iterator to the start.
         * 
         * @throws DbException           when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Returns the TupleDesc associated with this OpIterator.
         * 
         * @return the TupleDesc associated with this OpIterator.
         */
        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        /**
         * Closes the iterator. When the iterator is closed, calling next(),
         * hasNext(), or rewind() should fail by throwing IllegalStateException.
         */
        @Override
        public void close() {
            itr = null;
        }

    }

    public OpIterator iterator() {
        // some code goes here
        return new AggregateIterator();
    }

}
