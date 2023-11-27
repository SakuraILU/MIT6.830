package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private static enum LockType {
        SHARED, Exclusive
    };

    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, LockType>> locksMap;

    public boolean acquireLock(TransactionId tid, PageId pid, LockType type) {
        synchronized (locksMap) {

            // if this pid has no locks, create one according to type
            if (!locksMap.containsKey(pid)) {
                ConcurrentHashMap<TransactionId, LockType> locks = new ConcurrentHashMap<TransactionId, LockType>();
                locks.put(tid, type);
                locksMap.put(pid, locks);
                return true;
            }

            ConcurrentHashMap<TransactionId, LockType> locks = locksMap.get(pid);
            // if tid already has a lock
            // Important bug, if we want to modify some data in sets, use
            // for(int...){set.add/remove} or ListIterator.... while(itr.hasnext()){element
            // = itr.next(); itr.remove/ itr.add}
            // for(element: set){set.add/remove} will cause ConcurrentModification trouble
            LockType lktype = locks.get(tid);
            if (lktype != null) {
                // if relock the same lock, return true
                if (type == lktype) {
                    return true;
                    // if different, just want a SHARED lock, but already has a EXCLUSIVE lock
                } else if (type == LockType.SHARED) {
                    return true;
                    // if different, want a EXCLUSIVE lock, but already has a SHARED lock
                } else if (type == LockType.Exclusive) {
                    // check whether only this tid has a lock on this page
                    if (locks.size() == 1) {
                        locks.put(tid, type); // upgrade to EXCLUSIVE lock
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            // this tid don't has a lock
            else {
                if (type == LockType.SHARED) {
                    for (TransactionId id : locks.keySet()) {
                        if (locks.get(id) == LockType.Exclusive)
                            return false;
                    }
                    locks.put(tid, type);
                    return true;
                } else {
                    if (locks.size() == 0) {
                        locks.put(tid, type);
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            return true;

        }
    }

    // tid releases the lock on this page
    public boolean releaseLock(TransactionId tid, PageId pid) {
        synchronized (locksMap) {

            if (!locksMap.containsKey(pid))
                return true;

            ConcurrentHashMap<TransactionId, LockType> locks = locksMap.get(pid);
            if (locks == null)
                return true;

            // one tid only has one lock at most according to LockManaer.acquireLock(), so
            // if found one lock of this tid, just remove it and return
            LockType lktype = locks.get(tid);
            if (lktype != null) {
                locks.remove(tid);
                // if locks of this pid is empty, remove locks Vector for save resources
                if (locks.size() == 0)
                    locksMap.remove(pid);
                return true;
            }

            return false;
        }
    }

    // realse all locks of a transaction
    public synchronized boolean releaseAllLocks(TransactionId tid) {
        synchronized (locksMap) {
            for (PageId pid : locksMap.keySet()) {
                releaseLock(tid, pid);
            }
            return true;
        }

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized (locksMap) {
            ConcurrentHashMap<TransactionId, LockType> locks = locksMap.get(pid);
            if (locks == null)
                return false;

            LockType lktype = locks.get(tid);
            if (lktype != null)
                return true;

            return false;
        }
    }

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final long TIMEOUT = 2000;
    private static final int TIMEOUTOFFSET = 1000;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    // HashMap: pageId --> Page
    private ConcurrentHashMap<PageId, Page> pagesInBuffer;
    private ConcurrentHashMap<PageId, Long> pagesAge;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pagesInBuffer = new ConcurrentHashMap<PageId, Page>();
        this.pagesAge = new ConcurrentHashMap<PageId, Long>();
        this.locksMap = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, LockType>>();
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
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     *             (to be more accurately, what we gonna to deal with this page,
     *             locks are set according to out purpose)
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        LockType type = LockType.SHARED;
        if (perm == Permissions.READ_ONLY)
            type = LockType.SHARED;
        else if (perm == Permissions.READ_WRITE)
            type = LockType.Exclusive;
        else
            assert false : "Unsupported Permission type";

        // Acquire locks in a round-robin fashion, and report an error if it times out
        // VERY IMPORTANT NOTE: timeout limit need to be kind of random!!!!!!!!!
        // Reason:
        // otherwise, if two deadlocked tid t1 and t2 acquire this lock almost at the
        // same time, after a fixed time, t1 throw an error first and being restart
        // (will release its lock). if the lock causing deadlock is not realsed yet and
        // then context is switched to t2, t2 still deadlocked and throw an error, t2
        // will be restart too..... athough deadlock will be solved soon
        //
        // they both restart almost at the same time, acquire locks almost at the same
        // time, repeat the process above again and agin, they will be restart and
        // restart and restart...
        //
        // so random time can solve this problem, because they won't be almost the same
        // time even if the acquire the lock causing deadlock situation at the same time
        // even if the do both restart when acquire at the same time and timeout is the
        // same unluckily,
        // after several restart, the timeout should be different...otherwise you can
        // buy some stock :)
        long startTime = System.currentTimeMillis();
        long timeout = TIMEOUT + new Random().nextInt(TIMEOUTOFFSET);
        // System.out.println("try to get page " + pid.getPageNumber());
        while (!acquireLock(tid, pid, type)) {
            // System.out.println("try to get lock...");
            if (System.currentTimeMillis() - startTime > timeout)
                throw new TransactionAbortedException();

            // do 200ms sleep to avoid overcrowding when many transactions applying for
            // locks at the same time and to save CPU resources
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // see if this page is in hashtable, if not, read out this page and
        // store <pid, page> in hashtable
        if (!pagesInBuffer.containsKey(pid)) {
            if (pagesInBuffer.size() >= numPages)
                evictPage();
            // found this Dbfile in Catlog according to pid.tableId
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page pageFromDisk = file.readPage(pid);
            assert pid == pageFromDisk.getId() : "pid is not equal in getPage";
            pagesInBuffer.put(pid, pageFromDisk);
        }

        pagesAge.put(pid, System.currentTimeMillis());
        return pagesInBuffer.get(pid);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        // System.out.println("traverse page");
        for (Page page : pagesInBuffer.values()) {
            // A very IMPORTANT bug: in Steal mode, dirty pages may be evicted into disk and
            // become "clean" during a transcation...so we need to setBeforeImage of them
            // when commit and recover them when abort. A easy way is that setBeforeImage or
            // recover all pages related to this tid....
            if (commit) {
                // if commit, flush dirty page of this tid into disk
                try {
                    // if dirty, means their change is not flush into disk
                    if (page.isDirty() == tid) {
                        flushPage(page.getId());
                    }
                    // use current page contents as the before-image
                    // for the usage of the next transaction that modifies this page.
                    page.setBeforeImage(); // for all pages related to this tid, not only pages with dirty mark
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                // discard all changes made by this tid
                discardPage(page.getId());
            }

        }

        // realse the lock that tid holds on this page
        releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pagesModified = (ArrayList<Page>) f.insertTuple(tid, t);
        for (Page page : pagesModified) {
            page.markDirty(true, tid);
            // BTree has its local cache...so f.deleteTuple may not always use
            // BufferPool.getPage() like HeapFile, so we need to update them in
            // bufferpool's cache here
            pagesInBuffer.put(page.getId(), page);
            pagesAge.put(page.getId(), System.currentTimeMillis());
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
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pagesModified = (ArrayList<Page>) f.deleteTuple(tid, t);
        for (Page page : pagesModified) {
            page.markDirty(true, tid);
            // BTree has its local cache...so f.deleteTuple may not always use
            // BufferPool.getPage() like HeapFile, so we need to update them in
            // bufferpool's cache here
            pagesInBuffer.put(page.getId(), page);
            pagesAge.put(page.getId(), System.currentTimeMillis());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page : pagesInBuffer.values()) {
            flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pagesInBuffer.remove(pid);
        pagesAge.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        // append an update record to the log, with
        // a before-image and after-image.
        Page page = pagesInBuffer.get(pid);
        assert page != null : "flush a null page";

        TransactionId dirtier = page.isDirty();
        if (dirtier != null) {
            // make sure record log first and then flush into disk
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force(); // force() tell file system that write all pages of logfile into disk
                                           // immediately, don't cache page in the bufferpool of file system
            DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
            f.writePage(page);
            page.markDirty(false, null); // mark clean
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page : pagesInBuffer.values()) {
            if (page.isDirty() == tid) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        long lru = Long.MAX_VALUE;
        Page lruPage = null;
        for (Page page : pagesInBuffer.values()) {
            // found the least recent used page
            long timestamp = pagesAge.get(page.getId());
            if (timestamp < lru) {
                lruPage = page;
                lru = timestamp;
            }
        }

        if (lruPage != null) {
            try {
                // if dirty, flush into disk
                if (lruPage.isDirty() != null)
                    flushPage(lruPage.getId());
                // evict page from bufferpool
                discardPage(lruPage.getId());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return;
        }

        throw new DbException("All pages in the BufferPool are dirty, can not evict any page");
    }

}
