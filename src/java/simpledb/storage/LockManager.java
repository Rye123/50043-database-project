package simpledb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Graph that allows us to track dependencies.
 * If a transaction is waiting for a lock, it is considered 'dependent' on the various transactions
 * that hold a readlock or writelock on the lock.
 * 
 * A deadlock is detected when a dependent cycle is detected, using depth-first search on the graph.
 */
class LockGraph {
    // Each element is a transaction, linked to a list of locks they are waiting for
    private ConcurrentHashMap<TransactionId, List<MyFirstLock>> waitFor;

    public LockGraph() {
        waitFor = new ConcurrentHashMap<>();
    }

    public void addWaitFor(TransactionId tid, MyFirstLock lock) {
        if (!waitFor.containsKey(tid))
            waitFor.put(tid, new ArrayList<>());
        waitFor.get(tid).add(lock);
    }

    public void removeWaitFor(TransactionId tid, MyFirstLock lock) {
        if (waitFor.containsKey(tid))
            waitFor.get(tid).remove(lock);
    }

    private boolean dfsVisit(TransactionId node, List<TransactionId> visited) {
        if (visited.contains(node))
            return false;
        visited.add(node);
        boolean hasNoCycle = true;
        List<MyFirstLock> locks = waitFor.get(node);
        if (locks == null || locks.size() == 0) // not waiting for any lock
            return true;

        // loop through all the locks we're waiting for
        for (MyFirstLock lock : locks) {
            if (lock.isWriteLocked() && !lock.holdsWriteLock(node)) {
                // if there's a write lock, we only consider that one.
                TransactionId lockOwner = lock.getWriteLockHolder();
                if (visited.contains(lockOwner))
                    return false; // don't bother going further, we found a CYCLE
                
                // otherwise, we can dig deeper
                visited.add(lockOwner);
                hasNoCycle = hasNoCycle && dfsVisit(lockOwner, visited);
            }
            
            if (lock.isReadLocked()) {
                // scan through the read locks.
                for (TransactionId readOwner : lock.getReadLockHolders()) {
                    if (readOwner.equals(node))
                        continue;

                    if (visited.contains(readOwner))
                        return false; // don't bother going further, we found a CYCLE
                    
                    // otherwise, we can dig deeper
                    visited.add(readOwner);
                    hasNoCycle = hasNoCycle && dfsVisit(readOwner, visited);
                }
            } 
        }

        return hasNoCycle;
    }

    public boolean hasCycle() {
        ArrayList<TransactionId> nodes = new ArrayList<>(waitFor.keySet());
        ArrayList<TransactionId> visited = new ArrayList<>();

        boolean hasNoCycle = true;
        for (TransactionId node : nodes) {
            if (visited.contains(node)) {
                // already visited this node either directly or through dfs
                continue;
            } else {
                hasNoCycle = hasNoCycle && dfsVisit(node, visited);
            }
        }
        return !hasNoCycle;
    }
}

public class LockManager {
    private ConcurrentHashMap<PageId, MyFirstLock> pageLocks;
    private LockGraph graph;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
        this.graph = new LockGraph();
    }

    private void ensurePageInitialised(PageId pid) {
        if (!pageLocks.containsKey(pid)) {
            pageLocks.put(pid, new MyFirstLock());
        }
    }

    public boolean hasReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        return pageLocks.get(pid).holdsReadLock(tid);
    }

    public boolean hasWriteLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        return pageLocks.get(pid).holdsWriteLock(tid);
    }

    public void getReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        ensurePageInitialised(pid);
        MyFirstLock lock = pageLocks.get(pid);
        // if page is write locked, add self to the waitlist
        if (lock.isWriteLocked())
            graph.addWaitFor(tid, lock);

        // if that caused a deadlock, break out
        if (graph.hasCycle()) {
            graph.removeWaitFor(tid, lock);
            throw new TransactionAbortedException();
        }

        lock.acquireReadLock(tid);

        // We got the lock, so not waiting anymore
        graph.removeWaitFor(tid, lock); // it's ok if the waitfor doesn't exist i.e. lock was only readlocked
    }

    public void releaseReadLock(TransactionId tid, PageId pid) {
        ensurePageInitialised(pid);
        pageLocks.get(pid).releaseReadLock(tid);
    }

    public void getWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        ensurePageInitialised(pid);

        // if page is readlocked (or writelocked), add self to waitlist
        MyFirstLock lock = pageLocks.get(pid);
        if (lock.isReadLocked() || lock.isWriteLocked())
            graph.addWaitFor(tid, lock);
        
        // if that caused a deadlock, break out break oUT breAK OUT BREAK OUT B̷̛̺͍R̵͚̋E̸̡͕͗̐Ā̸̙K̷̦̊̇ ̵̳̋̚O̵͚̒͒U̷͓͝T̷̜̀̇ B̶̡̻̣͇̹̮̬͍̬̿̅̍̆͝R̷̢̨͎͇̦͕͕̥̼͋̋́̽̌̌́Ė̵̡͕̞̞̮͂͆Á̶̰͙͚̽͋̃͑̕͝K̵̢̤̭̭̜̮͑̉̽̀̿͝ ̶̮͔̠̯͙̥͊̌̾̈́́̈͊̓̚͠Ơ̶͈̋͒͗̈́̓̀̂Ŭ̷̢̨̲̣̦̞̊̊̋̆T̸̜͓̮͙̺̺̲̋̓̇̊́̑ B̴͖͎̼̭̦̭͈̣͉̮̺̘̞̻̬̮̮̙̮̠̭̺͇͖̼͎̣̰̫͕̖͕̝͚̪̪̋͋̈̔͑̑ͅR̵̨̡̧̛̛̛͚͉̳͉̰͇͖͉̖͈͈̪͇͔̦̣̙̲̜̫̮͎͙̫̹̪̩̹̙̱̱͓͍̼̪̜̯̣̣̗̪̉̏͐̿̑̆̌̀͗́̑̔̍̔͐̊͗͆̆̀̋͐͐̇̀̌̆̇̇͘̕̚͜͝͝͠ͅE̴̠̥̭̰̜̼͎̘͊͆̽̓̾͛̀̑̅̿̇͗̇̓͑̈̂̈̏͋͆̒̏̈̀́͌̕̚̕̕͝͝Ã̶̢̧̢̹̩̜̭̯̪̙͔̭͓̯̹͙̣̫̮̼̱̺̉͆͌̋͂̿͘Ķ̵̨̬̤̲̯̪̫̖̱̳̥̣͕̖͔͇͕̺̬͈̜̯͓̼͚̞̠̯̳̮̩̙̮̳͖̬͚̼̟͚̗̬͕̘̟͉͒͌̔͐̈́̆̌̈́̐͌̽̃̌͑̾̈͗̀̓͆͑̉̀̒̑̚͠͠ ̶̧̡̧̢̛̗͙̤̜̱̰͔͓͖̖̠͙̰̋̈́̓̄̌̇͊͌͑̿̉̊́̓̅̈́͂̃͂̒̈́̇͐̉͊͒͊̿̓̄̏͌͗̏͑̈̚̕͘̚̚̕͠͝O̴̻̲͙̪͇͚̻̙̪̮̮̲͖̯͔̬̩̠̻̿̓̔͊̏̿͌̒́͋͒̍͋̇̉̀͂̈̎̚͜͝͝ͅŮ̴̢̡̩͈̣̬̼̲̻̞̝̩̞̝̣̀̅̏̆̋̓̈̊̀̃̂͌͆̽̈́̈́͛̈́̉͂̿̅̆̚͘͘ͅT̶̞̘͇̝̫̰̓̀̈́̀̔̆̋̈́̈́̃̍̑̋̆́͌̀
        if (graph.hasCycle()) {
            graph.removeWaitFor(tid, lock);
            throw new TransactionAbortedException();
        }

        pageLocks.get(pid).acquireWriteLock(tid);

        // We got the lock, so not waiting
        graph.removeWaitFor(tid, lock);
    }

    public void releaseWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException{
        ensurePageInitialised(pid);
        if (!pageLocks.get(pid).holdsWriteLock(tid)) { // what the hell you're not the holder why do you have the write lock
            graph.removeWaitFor(tid, pageLocks.get(pid));
            throw new TransactionAbortedException(); //ABORT ABORT ABORT ABORT ABORT
        }
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

    /**
     * Releases all locks associated with transaction `tid`.
     * @param tid
     */
    public void releaseLocks(TransactionId tid) {
        for (MyFirstLock pageLock : pageLocks.values()) {
            if (pageLock.holdsReadLock(tid))
                pageLock.releaseReadLock(tid);
            if (pageLock.holdsWriteLock(tid))
                pageLock.releaseWriteLock(tid);
        }
    } 

    /**
     * Return if there is any locks on the page, and if not locked, remove the lock from the pageLocks hashmap
     * @param pid Page Id of the page to be checked
     * @return If there is a lock on the page
    */
    public synchronized boolean checkIfPageLocked(PageId pid) {
        MyFirstLock lock = pageLocks.get(pid);

        if (lock == null) {
            return false;
        }

        if (lock.isReadLocked()){
            return false;
        } else {
            pageLocks.remove(pid);
            return true;
        }
        
    }
}
