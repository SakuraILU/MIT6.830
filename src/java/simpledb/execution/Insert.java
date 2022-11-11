package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator[] children;
    private int tableId;
    private int insertCnt;
    private TupleDesc td;
    private boolean calledNext;

    /**
     * Constructor.
     *
     * @param t
     *                The transaction running the insert.
     * @param child
     *                The child operator from which to read tuples to be inserted.
     * @param tableId
     *                The table in which to insert tuples.
     * @throws DbException
     *                     if TupleDesc of child differs from table into which we
     *                     are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.children = new OpIterator[] { child };
        this.tableId = tableId;
        this.insertCnt = 0;
        this.td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "InsertedCount" });
        this.calledNext = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here

        // after open(), next() method should return total tuple number deleted
        // according to test case. thus, all tuples in the child should insert into
        // table in the open()
        super.open();
        children[0].open();
        while (children[0].hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, children[0].next());
                insertCnt++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        // some code goes here
        children[0].close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (calledNext) // if called more than once, return null
            return null;
        Tuple ret = new Tuple(this.td);
        ret.setField(0, new IntField(insertCnt));
        calledNext = true;
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }
}
