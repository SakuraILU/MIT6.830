package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
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

    File f;
    TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        // System.out.println(String.format("filesize is %d", f.length()));
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        // some code goes here
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // each page needs to be complete
        int pgsize = BufferPool.getPageSize();
        // if this page is a complete page, the start of next page should <= f.length()
        if (pgsize * (pid.getPageNumber() + 1) > f.length()) {
            throw new IllegalArgumentException(
                    String.format("pageId %d is invalid, out of file range %d", pid.getPageNumber(), f.length()));
        }

        byte rawbytes[] = new byte[pgsize];
        RandomAccessFile randomFile;
        try {
            randomFile = new RandomAccessFile(f, "r");
            randomFile.seek(pgsize * pid.getPageNumber()); // set read position of the randomReader
            randomFile.read(rawbytes, 0, pgsize); // arg1: bytes array to be write into arg2: start offset of the
                                                  // bytes[] arg3: write length
            randomFile.close();
            return new HeapPage((HeapPageId) pid, rawbytes); // convert the page bytestream into a HeadPage
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            System.out.println(String.format("[indexoutofbounds]: pageId %d, while file range %d", pid.getPageNumber(),
                    f.length()));
        }

        throw new IllegalArgumentException(
                String.format("Page %d is invalid, out of page range %d", pid.getPageNumber(), numPages()));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        // convert HeapPage into bytestream data and write into file (position is pgsize
        // * pgNo)
        byte[] pageData = page.getPageData();
        RandomAccessFile randomFile;
        try {
            randomFile = new RandomAccessFile(f, "rw");
            randomFile.seek(BufferPool.getPageSize() * page.getId().getPageNumber()); // set read position of the
                                                                                      // randomReader
            randomFile.write(pageData, 0, BufferPool.getPageSize());
            randomFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    // a pretty interesting note: We may extend HeapFile class into some more
    // advance class,
    // e.g. give it a tuple t, it insert t in every page of this table...
    // so the abstrac class returns List<Page> rather than a single page
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        ArrayList<Page> pageArray = new ArrayList<Page>();

        for (int pgNo = 0; pgNo < numPages(); ++pgNo) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo),
                    Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                pageArray.add(page);
                return pageArray;
            }
            // Important Note: We must release the lock on the page, bcz we actually don't
            // use it, no read/write data at all...
            // otherwise we scan many pages here to find a page with empty slot, if we don't
            // release their locks, many other transactions will be blocked...cuase some
            // situations very hard to understand
            Database.getBufferPool().unsafeReleasePage(tid, page.getId());
        }

        // all pages in the file is full, append a new empty page in the file
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        FileOutputStream filewriter = new FileOutputStream(f, true); // append: true
        try {
            filewriter.write(emptyPageData);
        } catch (IOException e) {
            filewriter.close();
            e.printStackTrace();
        }
        filewriter.close();

        // malloc a new HeapPage in memory, becuase it is also empty as the new page in
        // the file, don't need to write this HeapPage into disk
        HeapPage page = new HeapPage(new HeapPageId(getId(), numPages() - 1), emptyPageData);
        page.insertTuple(t);
        pageArray.add(page);
        return pageArray;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageArray = new ArrayList<Page>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.recordId.getPageId(),
                Permissions.READ_WRITE);
        page.deleteTuple(t);
        pageArray.add(page);
        return pageArray;
        // not necessary for lab1
    }

    // wrapper HeapPageIterator into HeapFileIterator
    // iterate all tuples sequentially in the DBFile
    private class HeapFileIterator implements DbFileIterator {
        private int nextPgNo;
        private TransactionId tid;
        private Iterator<Tuple> tupleItr; // the iterator of one HeapPage (iterate it's tuples)

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        // use open to initialize, if not open, hasNext() and next() shouldn't fail and
        // throw error!
        @Override
        public void open() throws DbException, TransactionAbortedException {
            nextPgNo = 0;
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), nextPgNo++),
                    Permissions.READ_ONLY);
            tupleItr = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // unopened yet
            if (tupleItr == null)
                return false;

            // the current page has been iterated over
            // hasNext() method detemine whether has next tuple, so here must
            // has next page if current page is iterated over when next() is called

            // possibly, a whole page is empty, so here need loops to find next page with
            // tuples
            while (!tupleItr.hasNext()) {
                if (nextPgNo >= numPages())
                    return false;
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), nextPgNo++),
                        Permissions.READ_ONLY); // should iterate the next page
                tupleItr = page.iterator();
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException {
            // unopened yet
            if (tupleItr == null)
                throw new NoSuchElementException("HeapFileIterator is not opened yet.");

            return tupleItr.next(); // return next tuple
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleItr = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}