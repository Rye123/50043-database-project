package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.swing.plaf.TreeUI;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Tick for recording LRU 'timestamps' -- this will be incremented every operation.
     */
    private int tick;

    /**
     * Associated a page ID with a Page.
     */
    private ConcurrentHashMap<PageId, Page> pages;
    private ConcurrentHashMap<PageId, Integer> lastUsed;
    private LockManager lockManager;
    /**
     * Max number of pages in buffer pool.
     */
    private int numPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.pages = new ConcurrentHashMap<>();
        this.lastUsed = new ConcurrentHashMap<>();
        this.numPages = numPages;
        this.lockManager = new LockManager();
        this.tick = 0;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
    throws TransactionAbortedException, DbException {
        if (perm == Permissions.READ_ONLY) {
            lockManager.getReadLock(tid, pid);
        } else if (perm == Permissions.READ_WRITE) {
            lockManager.getWriteLock(tid, pid);
        }
        
        this.tick++;
        // Check if in buffer pool
        if (pages.containsKey(pid)) {
            Page page = pages.get(pid);
            lastUsed.put(pid, tick);
            return page;
        }
        
        // Check if sufficient space -- if not enough, EVICT
        if (pages.size() >= numPages) {
            evictPage();
        }
        // look for the page -- loop through catalog tables until we find a catalog with the file with the page
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pages.put(pid, page);
        lastUsed.put(pid, tick);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        if (lockManager.hasReadLock(tid, pid))
            lockManager.releaseReadLock(tid, pid);
        
        if (lockManager.hasWriteLock(tid, pid))
            lockManager.releaseWriteLockExceptionless(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);         // Just commit directly
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.hasWriteLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        switch (commit) {
            case true:
            // Flush dirty pages associated to the disk
            
                break;

            case false:

                // Restore the page to on-disk state
                break;
        }

        // release any locks acquired 
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        tick++;
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifiedPages = file.insertTuple(tid, t);
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);

            this.pages.put(page.getId(), page); // overwrite old copy of the page
            lastUsed.put(page.getId(), tick);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        tick++;
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifiedPages = file.deleteTuple(tid, t);
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);

            this.pages.put(page.getId(), page); // overwrite old copy of the page
            lastUsed.put(page.getId(), tick);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pages.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        if (this.pages.contains(pid)) {
            this.pages.remove(pid);
            this.lastUsed.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // write dirty page to disk, and mark it as not dirty
        Page page = this.pages.get(pid);

        if (this.pages.containsKey(pid)) {
            TransactionId dirtyTid = page.isDirty();

            if (dirtyTid != null) {
                DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
                f.writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        PageId lruPid = null;
        int lowestLastUsedVal = Integer.MAX_VALUE;

        // Get LRU Page
        for (PageId pid : this.pages.keySet()) {
            Page thisPage = this.pages.get(pid);
            if (thisPage.isDirty() != null) {           // If this page is dirty
                continue;                               // Do not count this page
            }
            int lastUsed = this.lastUsed.get(pid);
            if (lowestLastUsedVal > lastUsed) {
                lowestLastUsedVal = lastUsed;
                lruPid = pid;
            }
        }
        if (lruPid == null)
            throw new DbException("nothing to evict.");

        Page lruPage = this.pages.get(lruPid);
        if (lruPage.isDirty() != null) {
            // page is dirty, we cannot flush this page
            throw new DbException("page could not be yeeted. y u bully me");
        }

        // Detect if there is a lock in the page
        if (!lockManager.removeLockforPage(lruPid)) {
            throw new DbException("BufferPool.evictPage: the page is being used");
        }

        // YEET HIM OUT
        this.pages.remove(lruPid);
        this.lastUsed.remove(lruPid);
        // TODO: add this part into a critical section
    }

}
