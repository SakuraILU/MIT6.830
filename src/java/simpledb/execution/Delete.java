package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator[] children;
    private TupleDesc td;

    private int deleteCnt;
    private boolean calledNext;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *              The transaction this delete runs in
     * @param child
     *              The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.children = new OpIterator[] { child };
        this.td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "DeleteCount" });
        this.calledNext = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here

        // after open(), next() method should return total tuple number inserted
        // according to test case. thus, all tuples in the child should insert into
        // table in the open()
        super.open();
        children[0].open();
        while (children[0].hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, children[0].next());
                deleteCnt++;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (calledNext) // if called more than once, return null
            return null;
        Tuple ret = new Tuple(td);
        ret.setField(0, new IntField(deleteCnt));
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
