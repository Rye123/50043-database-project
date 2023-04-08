package simpledb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class LockManager {
    private ConcurrentHashMap<PageId, StampedLock> pageLocks;
    private ConcurrentHashMap<PageId, TransactionId> writeLockHolders;
    private ConcurrentHashMap<PageId, List<TransactionId>> readLockHolders;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
        this.writeLockHolders = new ConcurrentHashMap<>();
        this.readLockHolders = new ConcurrentHashMap<>();
    }

    private void ensurePageInitialised(PageId pid) {
        if (!pageLocks.containsKey(pid)) {
            pageLocks.put(pid, new StampedLock());
            readLockHolders.put(pid, new ArrayList<>());
            // writeLockHolders.put(pid, null);
        }
    }

    private void ensurePageUninitialized(PageId pid) {
        if (pageLocks.containsKey(pid)) {
            pageLocks.remove(pid);
        }
    }

    private boolean hasOtherReaders(TransactionId tid, PageId pid) {
        for (TransactionId otherTid : readLockHolders.get(pid)) {
            if (!otherTid.equals(tid))
                return true;
        }
        return false;
    }

    public boolean hasReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        if (!readLockHolders.containsKey(pid))
            return false;

        return readLockHolders.get(pid).contains(tid);
    }

    public boolean hasWriteLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        if (!writeLockHolders.containsKey(pid))
            return false;
        
        TransactionId otherTid = writeLockHolders.get(pid);
        return (!(otherTid == null) && otherTid.equals(tid));
    }

    public void getReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        if (hasReadLock(tid, pid))
            return;

        if (hasWriteLock(tid, pid)) // you already have a write lock idiot what you doing
            return;

        pageLocks.get(pid).asReadLock().lock();
        readLockHolders.get(pid).add(tid);
    }

    public void releaseReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        readLockHolders.get(pid).remove(tid);
        pageLocks.get(pid).asReadLock().unlock();
    }

    public void getWriteLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);

        if (hasWriteLock(tid, pid)) // you already have a write lock dumbass what you doin
            return;
        
        if (hasReadLock(tid, pid)) { // you have a read lock
            if (pageLocks.get(pid).tryConvertToWriteLock(0) != 0)
                writeLockHolders.put(pid, tid);
            return;
        }

        pageLocks.get(pid).asWriteLock().lock();
        writeLockHolders.put(pid, tid);
    }

    public void releaseWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException{
        ensurePageInitialised(pid);
        if (!writeLockHolders.get(pid).equals(tid)) // what the hell you're not the holder why do you have the write lock
            throw new TransactionAbortedException(); //ABORT ABORT ABORT ABORT ABORT
        writeLockHolders.remove(pid);
        pageLocks.get(pid).asWriteLock().unlock();
    }

    /**
     * Releases the write lock without an exception (releaseWriteLock throws an exception if the caller doesn't have the write lock)
     * @param tid
     * @param pid
     */
    public void releaseWriteLockExceptionless(TransactionId tid, PageId pid){
        ensurePageInitialised(pid);
        writeLockHolders.remove(pid);
        pageLocks.get(pid).asWriteLock().unlock();
    }
}
