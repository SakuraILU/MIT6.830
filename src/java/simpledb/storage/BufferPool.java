package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;
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

    private class LockManager {
        private class Lock {
            public TransactionId tid; // which transcation holds this lock
            public LockType type; // SHARED or EXCLUSIVE lock

            Lock(TransactionId tid, LockType type) {
                this.tid = tid;
                this.type = type;
            }

            @Override
            public boolean equals(Object obj) {
                if (!this.getClass().isInstance(obj)) {
                    return false;
                }
                Lock other = (Lock) obj;

                return (this.tid == other.tid) && (this.type == other.type);
            }

            @Override
            public String toString() {
                return tid.toString() + "(" + type.toString() + ")  ";
            }
        }

        private ConcurrentHashMap<PageId, Vector<Lock>> locksMap; // lab4 introduces multi-thread, so ConcurrentHashMap
                                                                  // would be a better choice, bcz we don't need to lock
                                                                  // the whole locksMap for modification

        LockManager() {
            locksMap = new ConcurrentHashMap<PageId, Vector<Lock>>();
        }

        public boolean acquireLock(TransactionId tid, PageId pid, LockType type) {
            // if this pid has no locks, create one according to type
            //
            // Thread-safe: two threads may enter this if statement (locksmap only protect
            // thread-safe inside itself). they may add locks for the same pid one by one
            // through locksMap.put(), thus one locks will be override and makes the lock
            // inside vanished...
            // so, we need to lock the pid, makes only one thread can add locks for one pid
            synchronized (pid) {
                if (!locksMap.containsKey(pid)) {
                    Vector<Lock> locks = new Vector<Lock>();
                    locks.add(new Lock(tid, type));
                    locksMap.put(pid, locks);
                    return true;
                }
            }

            Vector<Lock> locks = locksMap.get(pid);
            // thread-safe: we need to add lock in locks(Vector, add and remove need to
            // operate multi lock at the same time, so need to protect locks) and also read
            // its property locks.size(), concurrency for the same locks is prohibited
            synchronized (locks) {
                // if tid already has a lock
                for (Lock lock : locks) {
                    if (lock.tid == tid)
                        // 1. exactly same
                        // 2. need Shared lock but has exclusive lock, exclusive lock supports read, so
                        // just return true to say: ok, you can write
                        if (lock.type == type || lock.type == LockType.Exclusive)
                            return true;
                        // need Exclusive lock but has a shared lock, if this pid only has this shared
                        // lock, no other tid is try to read or write, we can upgrade this
                        // shared->exlucisve, bcz we don't effect any one...
                        else if (locks.size() == 1) {
                            locks.get(0).type = LockType.Exclusive;
                            return true;
                        }
                }

                // this tid don't has a lock

                if (type.equals(LockType.Exclusive)) {
                    // on other tid is read or write, we can write
                    if (locks.size() == 0) {
                        locks.add(new Lock(tid, type));
                    } else
                        return false;
                } else if (type.equals(LockType.SHARED)) {
                    // on other tid is try to write, we can read
                    for (Lock lock : locks) {
                        if (lock.type == LockType.Exclusive)
                            return false;
                    }
                    locks.add(new Lock(tid, type));
                }
            }
            return true;
        }

        // tid releases the lock on this page
        public boolean releaseLock(TransactionId tid, PageId pid) {
            if (!locksMap.containsKey(pid))
                return true;

            Vector<Lock> locks = locksMap.get(pid);
            // thread-safe: we need to remove lock in locks (add and remove need to operate
            // multi lock at the same time, so need to protect locks) and read lock
            // property lock.tid (protect lock or even locks more aggresively)
            // so concurrency for the same locks is prohibited
            // note: locksMap.remove() is ok, bcz it's ConcurrentHashMap
            synchronized (locks) {

                // one tid only has one lock at most according to LockManaer.acquireLock(), so
                // if found one lock of this tid, just remove it and return
                for (Lock lock : locks) {
                    if (lock.tid == tid) {
                        locks.remove(lock);

                        // if locks of this pid is empty, remove locks Vector for save resources
                        if (locks.size() == 0)
                            locksMap.remove(pid);

                        return true;
                    }
                }
            }
            return false;
        }

        // if tid holds a lock on this page
        public boolean holdsLock(TransactionId tid, PageId pid) {
            Vector<Lock> locks = locksMap.get(pid);
            // thread-safe: we need to read lock property lock.tid
            // so concurrency for the same lock is prohibited
            for (Lock lock : locks) {
                synchronized (lock) {
                    if (lock.tid == tid) {
                        return true;
                    }
                }
            }
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
    private LockManager lockManager;

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
        this.lockManager = new LockManager();
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
        while (!lockManager.acquireLock(tid, pid, type)) {
            if (System.currentTimeMillis() - startTime > timeout)
                throw new TransactionAbortedException();

            // do 200ms sleep to avoid overcrowding when many transactions applying for
            // locks at the same time and to save CPU resources
            try {
                Thread.sleep(200);
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
        lockManager.releaseLock(tid, pid);
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

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.holdsLock(tid, p);
        return false;
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
        for (Page page : pagesInBuffer.values()) {
            // note: this tid will hold Xlock until commit or abort, so if a page is dirtied
            // by the tid, it won't be dirtied by others
            if (commit) {
                // if commit, flush dirty page of this tid into disk
                try {
                    if (page.isDirty() == tid) {
                        flushPage(page.getId());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                // if abort, recover this pages in memory from disk (store original data before
                // this transaction)
                if (page.isDirty() == tid) {
                    HeapFile dbfile = (HeapFile) Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                    Page pageFromDisk = dbfile.readPage(page.getId());
                    pagesInBuffer.put(pageFromDisk.getId(), pageFromDisk);
                    pagesAge.put(pageFromDisk.getId(), System.currentTimeMillis());
                }
            }

            // realse the lock that tid holds on this page
            lockManager.releaseLock(tid, page.getId());
        }
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
            pagesInBuffer.put(page.getId(), page);
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
        for (Page page : pagesModified)
            page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        // for (Page page : pagesInBuffer.values()) {
        // flushPage(page.getId());
        // }
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
        Page page = pagesInBuffer.get(pid);
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        f.writePage(page);
        pagesInBuffer.remove(page.getId());
        pagesAge.remove(page.getId());
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
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
            // don't evict dirty pages into disk...
            // bcz dirty page can only be flush into disk by commit
            // otherwise, abort can not recover original pages from disk
            if (page.isDirty() != null)
                continue;

            // found the least recent used (clean) page
            long timestamp = pagesAge.get(page.getId());
            if (timestamp < lru) {
                lruPage = page;
                lru = timestamp;
            }
        }

        // just remove in bufferpool bcz its clean
        if (lruPage != null) {
            pagesInBuffer.remove(lruPage.getId());
            return;
        }

        throw new DbException("All pages in the BufferPool are dirty, can not evict any page");
    }

}
