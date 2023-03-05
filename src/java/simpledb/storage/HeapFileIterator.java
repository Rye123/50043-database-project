package simpledb.storage;

import java.util.Iterator;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> currentTupleIterator;
    TransactionId transactionId;
    HeapFile heapFile;
    int currentPageNo;

    /**
     * Constructor for the iterator
     * @param hf
     * @param tid
     */
    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        heapFile = hf;
        transactionId = tid;
        currentPageNo = -1;
    }

    /**
     * Open the iterator, must be called before readNext.
     */
    public void open() throws DbException, TransactionAbortedException {
        currentPageNo = -1;
    }

    @Override
    public Tuple readNext() throws TransactionAbortedException, DbException {

        // Sanity check for current tuple
        if (currentTupleIterator != null && !currentTupleIterator.hasNext()) {
            currentTupleIterator = null;
        }

        // Iterate through pages and tuples in each page
        while (currentTupleIterator == null && currentPageNo < heapFile.numPages() - 1) {
            // Go next page first
            currentPageNo++;

            // Initialize iterator for this page
            HeapPageId currentPageId = new HeapPageId(heapFile.getId(), currentPageNo);

            HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(
                    transactionId,
                    currentPageId,
                    Permissions.READ_ONLY);

            currentTupleIterator = currentPage.iterator();

            // Check if still got item in this page
            if (!currentTupleIterator.hasNext()){
                currentTupleIterator = null;
            }
        }

        // If really dont have, then means really dont have alr
        if (currentTupleIterator == null){
            return null;
        }
        return currentTupleIterator.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
}
