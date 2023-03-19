package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.DbFile;
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
    private OpIterator child;
    private TupleDesc td;
    private boolean fetchNextCalledBefore;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Delete Count"});
        this.fetchNextCalledBefore = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.fetchNextCalledBefore = false;
        this.child.open();
        super.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.fetchNextCalledBefore = false;
        this.child.rewind();
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
        if (fetchNextCalledBefore)
            return null;
        int count = 0;
        while (child.hasNext()) {
            Tuple toDel = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, toDel);
                count++;
            } catch (IOException e) {
                throw new DbException("Could not write to file.");
            } catch (DbException e) {
                // do nothing if the child already is deleted
            }
        }
        // Construct tuple
        IntField countField = new IntField(count);
        Tuple countTup = new Tuple(td);
        countTup.setField(0, countField);
        this.fetchNextCalledBefore = true;
        return countTup;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0])
            this.child = children[0];
    }

}
