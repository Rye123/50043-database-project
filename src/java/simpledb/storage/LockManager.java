package simpledb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class LockManager {
    private ConcurrentHashMap<PageId, MyFirstLock> pageLocks;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
    }

    private void ensurePageInitialised(PageId pid) {
        if (!pageLocks.containsKey(pid)) {
            pageLocks.put(pid, new MyFirstLock());
        }
    }

    // private boolean hasOtherReaders(TransactionId tid, PageId pid) {
    //     for (TransactionId otherTid : readLockHolders.get(pid)) {
    //         if (!otherTid.equals(tid))
    //             return true;
    //     }
    //     return false;
    // }

    public boolean hasReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        return pageLocks.get(pid).holdsReadLock(tid);
    }

    public boolean hasWriteLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        return pageLocks.get(pid).holdsWriteLock(tid);
    }

    public void getReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        pageLocks.get(pid).acquireReadLock(tid);
    }

    public void releaseReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        pageLocks.get(pid).releaseReadLock(tid);
    }

    public void getWriteLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        pageLocks.get(pid).acquireWriteLock(tid);
    }

    public void releaseWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException{
        ensurePageInitialised(pid);
        if (!pageLocks.get(pid).holdsWriteLock(tid)) // what the hell you're not the holder why do you have the write lock
            throw new TransactionAbortedException(); //ABORT ABORT ABORT ABORT ABORT
        pageLocks.get(pid).releaseWriteLock(tid);
    }

    /**
     * Releases the write lock without an exception (releaseWriteLock throws an exception if the caller doesn't have the write lock)
     * @param tid
     * @param pid
     */
    public void releaseWriteLockExceptionless(TransactionId tid, PageId pid){
        ensurePageInitialised(pid);
        pageLocks.get(pid).releaseWriteLock(tid);
    }
}
