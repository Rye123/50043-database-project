package simpledb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class HeapFileIterator implements DbFileIterator {
    Iterator<Tuple> currentTupleIterator;
    TransactionId transactionId;
    HeapFile heapFile;
    int currentPageNo;
    private boolean isOpen;

    /**
     * Constructor for the iterator
     * @param hf
     * @param tid
     */
    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.heapFile = hf;
        this.transactionId = tid;
        this.currentTupleIterator = null;
        this.isOpen = false;
        this.currentPageNo = -1;
    }

    /**
     * Open the iterator, must be called before readNext.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.currentPageNo = 0;
        this.isOpen = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        // Not open
        if(!this.isOpen){
            return false;
        }
        // Find iterator 
        // System.out.println(this.currentTupleIterator==null);
        // System.out.println(this.currentPageNo);
        while (this.currentTupleIterator == null && this.currentPageNo <= this.heapFile.numPages()-1) {
            HeapPageId currentPageId = new HeapPageId(this.heapFile.getId(), this.currentPageNo);
            HeapPage currentPage = (HeapPage) this.heapFile.readPage(currentPageId);
            this.currentTupleIterator = currentPage.iterator();
            // Initialize iterator for this page
        }
        if (!this.currentTupleIterator.hasNext()){
            this.currentPageNo++;
            this.currentTupleIterator = null;
        }
        if (this.currentTupleIterator != null){
            if (this.currentTupleIterator.hasNext() && this.isOpen){
                return true;
            }
        }
        return false;
    }

    @Override
    public Tuple next() throws TransactionAbortedException, DbException {      
        if (!isOpen){
            throw new NoSuchElementException();
        }
        Tuple returnedTuple = this.currentTupleIterator.next();
        return returnedTuple;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    public void close() {
        this.isOpen = false;
    }
}
