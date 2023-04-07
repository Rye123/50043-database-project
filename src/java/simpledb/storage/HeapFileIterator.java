package simpledb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class HeapFileIterator extends AbstractDbFileIterator {
    Iterator<Tuple> curTupleIterator;
    TransactionId transactionId;
    HeapFile heapFile;
    int curPageNo;

    /**
     * Constructor for the iterator
     * @param hf
     * @param tid
     */
    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.heapFile = hf;
        this.transactionId = tid;
        this.curTupleIterator = null;
        this.curPageNo = 0;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // Initialise curTupleIterator
        if (this.curPageNo < this.heapFile.numPages()) {
            HeapPageId curPageId = new HeapPageId(this.heapFile.getId(), curPageNo);
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(transactionId, curPageId, Permissions.READ_ONLY);
            curTupleIterator = curPage.iterator();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        curTupleIterator = null;
        curPageNo = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // initialise tupleiter
        if (this.curTupleIterator == null) {
            return null;
        } 

        // Loop through pages
        while (true) {
            if (curTupleIterator.hasNext())
                return curTupleIterator.next();
            
            // Otherwise, go to next page
            if (this.curPageNo < this.heapFile.numPages() - 1) {
                this.curPageNo++;
                // we're now at any page from 0 to last page
                HeapPageId curPageId = new HeapPageId(this.heapFile.getId(), curPageNo);
                HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(transactionId, curPageId, Permissions.READ_ONLY);
                curTupleIterator = curPage.iterator();
            } else {
                // no more pages left -- we're at the last page
                break;
            }
        }
        return null;
    }
}
