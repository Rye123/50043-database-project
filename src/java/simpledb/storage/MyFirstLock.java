package simpledb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import simpledb.transaction.TransactionId;

/**
 * Lock that associates itself with a TransactionId
 */
public class MyFirstLock {
    private TransactionId writeLockHolder;
    private List<TransactionId> readLockHolders;
    private List<TransactionId> readLockHoldersWaitingToUpgrade; // read lock holders that want to upgrade to write lock
    private Semaphore writeLocked;

    public MyFirstLock() {
        writeLockHolder = null;
        readLockHolders = new ArrayList<>();
        readLockHoldersWaitingToUpgrade = new ArrayList<>();
        writeLocked = new Semaphore(1);
    }

    /**
     * Returns true if this page has any readers that are NOT waiting to
     * upgrade to write lock AND there's no one holding the write lock
     * 
     * We want to do this so those waiting for an upgrade can just go ahead (one at a time)
     * as long as there's no readers still perusing
     * 
     * This is to avoid a deadlock as described in https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock#Upgradable_RW_lock
     * - Here, we wait for those readers who haven't requested an upgrade to finish first before going through.
     * @return
     */
    private synchronized boolean canIUpgradeNow(TransactionId tid) {
        // if there's someone already in first class you can kindly go away
        if (writeLockHolder != null && !writeLockHolder.equals(tid))
            return false;
        
        // loop through the readlockholders, ensure none of them wanna upgrade
        for (TransactionId readLockHolder : readLockHolders) {
            if (readLockHolder.equals(tid)) // well of course I know him, he's me
                continue;
            
            if (!readLockHoldersWaitingToUpgrade.contains(readLockHolder)) {
                // there's still one reader in there doing his stuff not waiting for an upgrade
                return false;
            }
        }

        // everyone else is either a loser waiting for an upgrade or it's me (i.e. also a loser)
        return true;
    }

    /**
     * Returns true if this page has at least one read lock or write lock on it.
     * @return
     */
    public boolean isReadLocked() {
        return (readLockHolders.size() != 0) || isWriteLocked();
    }

    /**
     * Returns true if this page is exclusively locked (write-locked).
     * @return
     */
    public boolean isWriteLocked() {
        return writeLocked.availablePermits() == 0;
    }

    /**
     * Returns true if the given transaction holds this read lock.
     * @param tid
     * @return
     */
    public boolean holdsReadLock(TransactionId tid) {
        return readLockHolders.contains(tid);
    }

    /**
     * Returns true if the given transaction holds this write lock.
     * @param tid
     * @return
     */
    public boolean holdsWriteLock(TransactionId tid) {
        return (writeLockHolder != null && writeLockHolder.equals(tid));
    }

    /**
     * Acquires a read lock.
     * @param tid
     */
    public synchronized void acquireReadLock(TransactionId tid) {
        // if we already hold a read or write lock, then no need to do anything.
        if (holdsReadLock(tid) || holdsWriteLock(tid))
            return;
        
        while (isWriteLocked());
        
        readLockHolders.add(tid);
    }

    /**
     * Releases a read lock.
     * @param tid
     */
    public void releaseReadLock(TransactionId tid) {
        readLockHolders.remove(tid);
    }

    public void acquireWriteLock(TransactionId tid) {
        // if you already hold a write lock, why are you here?
        if (holdsWriteLock(tid))
            return;
        
        // if you already hold a read lock but not a write lock, we wanna upgrade you to first class
        if (holdsReadLock(tid)) {
            synchronized (this) {
                readLockHoldersWaitingToUpgrade.add(tid);
                while (!canIUpgradeNow(tid));
                
                readLockHoldersWaitingToUpgrade.remove(tid);
                try {
                    writeLocked.acquire();
                } catch (InterruptedException e) {}
                
                // manually release the read lock BUT DON'T TELL THE OTHERS YET
                readLockHolders.remove(tid);
                
                // now you can take your rightful place as mandalock
                writeLockHolder = tid;
                return;
            }
        }

        // otherwise, wait until all the other losers using the cubicle are done
        synchronized (this) {
            while(isReadLocked());
            try {
                writeLocked.acquire();
            } catch (InterruptedException e) {}
            writeLockHolder = tid;
        }
    }

    /**
     * Releases a write lock.
     * @param tid
     */
    public synchronized void releaseWriteLock(TransactionId tid) {
        if (writeLockHolder == tid) {
            writeLockHolder = null;
            writeLocked.release();
        }
    }

    public List<TransactionId> getReadLockHolders() {
        return readLockHolders;
    }

    public TransactionId getWriteLockHolder() {
        return writeLockHolder;
    }
}
