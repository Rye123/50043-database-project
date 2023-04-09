package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private int hashCode;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.hashCode = f.getAbsoluteFile().hashCode();
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.hashCode;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
       return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(this.file,"r");
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            if (offset + BufferPool.getPageSize() > randAccessFile.length()) {
                randAccessFile.close();
                throw new IOException("maximum allowed offset exceeded");
            }
            randAccessFile.seek(offset);
            randAccessFile.readFully(data);
            randAccessFile.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File specified not found");
        } catch (IOException e) {
            throw new IllegalArgumentException("Read write error");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile randAccessFile = new RandomAccessFile(this.file, "rw");
        int offset = BufferPool.getPageSize() * page.getId().getPageNumber();
        if (offset > randAccessFile.length()) {
            randAccessFile.close();
            throw new IOException("maximum allowed offset exceeded");
        }
        randAccessFile.seek(offset);
        randAccessFile.write(page.getPageData());
        randAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(this.file.length()/BufferPool.getPageSize());
    }

    private void createNewPage(HeapPageId pid) throws IOException {
        HeapPage newPage = new HeapPage(pid, HeapPage.createEmptyPageData());
        writePage(newPage);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // identify an empty page to write to
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for (int i = 0; i < this.numPages(); i++) {
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            // if empty, insert
            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); // escalate perms
                page.insertTuple(t);
                modifiedPages.add(page);
                return modifiedPages; // here we expect the lock to be released by the caller later.
            }
            Database.getBufferPool().unsafeReleasePage(tid, pid); // if nothing was inserted, we can safely release the lock
        }
        // no pages, create new one and insert
        HeapPageId newPid = new HeapPageId(this.getId(), this.numPages());
        createNewPage(newPid);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
        page.insertTuple(t);
        modifiedPages.add(page);

        if (modifiedPages.isEmpty())
            throw new DbException("failed to insert tuples to this file");
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> modifiedPages = new ArrayList<>();
        // identify the page the tuple is on
        RecordId rid = t.getRecordId();
        if (rid == null)
            throw new DbException("tuple has null rid");
        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        modifiedPages.add(page);
        if (modifiedPages.isEmpty())
            throw new DbException("failed to delete tuples from this file");
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

