package simpledb;

import java.io.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.locks.Lock;


class ConcurrencyMgr {
    enum LockType {
        slock,
        xlock,
    }

    private class LockObj{
        private LockType type;
        private PageId pageId;
        private ArrayList<TransactionId> lockList;

        public LockObj(LockType type, PageId pageId) {
            this.type = type;
            this.pageId = pageId;
            lockList = new ArrayList<>();
        }

        public LockType getType() {
            return type;
        }

        public boolean isSLock() {
            return type == LockType.slock;
        }

        public boolean hasSlock() {
            if(type == LockType.slock) {
                return true;
            } else {
                if(lockList.size() == 0) {
                    return true;
                } else {
                    return false;
                }
                    //return false;
            }
        }

        public TransactionId getFirst() {
            return lockList.get(0);
        }

        public int size() {
            return lockList.size();
        }
        public synchronized void addLockList(TransactionId tid) {
            // lockList
            lockList.add(tid);
        }
        public synchronized boolean upgrade(TransactionId tid) {
            if(size() == 1 && tid.equals(getFirst())) {
                type = LockType.xlock;
                return true;
            }
            assert (false);
            System.out.println("不应该到这里");
            return false;
        }

    }
    private ConcurrentHashMap<PageId, LockObj> lockTable;

    public ConcurrencyMgr() {
        lockTable = new ConcurrentHashMap<>();
    }

    public synchronized void requestSLock(TransactionId tid, PageId pid) {

        LockObj l = lockTable.getOrDefault(pid, null);
        if(l == null) {
            // 在该页上不存在lock，所以可以随便锁。
            l = new LockObj(LockType.slock, pid);
            l.addLockList(tid);
            lockTable.put(pid, l);
        } else {
            // 由于已经存在锁对象，需要进一步判断是否为Slock对象。
            if(l.isSLock()) {
                l.addLockList(tid);
            } else {
                try {
                    while( !l.hasSlock())
                        wait(1000);
                    assert (l.hasSlock());
                    l.addLockList(tid);

                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public synchronized void requestXlock(TransactionId tid, PageId pid) {
        LockObj l = lockTable.getOrDefault(pid, null);
        if(l == null) {
            l = new LockObj(LockType.xlock, pid);
            l.addLockList(tid);
            lockTable.put(pid, l);
        } else {
            if(l.size() == 1) {
                if(l.getFirst().equals(tid)) {
                    l.upgrade(tid);
                } else {
                    try{
                        while(l.hasSlock())
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }

        }
    }
    public synchronized void requestLock(LockType type, TransactionId tid, PageId pid) {
        LockObj l = lockTable.getOrDefault(pid, null);
        while(true) {
            if(l == null) {
                l = new LockObj(type, pid);
                l.addLockList(tid);
                lockTable.put(pid, l);
                return;
            }
            if(l.getType() == LockType.slock) {
                if(type == LockType.slock) {
                    l.addLockList(tid);
                    return;
                } else {

                }

            }



        }

    }

}

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

    // private Page[] pageBuffer;
    private HashMap<PageId, Page> pageBuffer;
    private ArrayList<PageId> pageIdList;
    private int currentSize;
    private int maxSize;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        // pageBuffer = new Page[numPages];
        pageBuffer = new HashMap<>();
        pageIdList = new ArrayList<>();
        currentSize = 0;
        maxSize = numPages;
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
        // some code goes here
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p = pageBuffer.getOrDefault(pid, null);
        if(p == null) {
          if(currentSize == maxSize) {
            // throw new DbException("currentSize == maxSize, no more space!");
              evictPage();
          }
          p = dbfile.readPage(pid);
          pageBuffer.put(pid, p);
          pageIdList.add(pid);
          currentSize++;
        }
        return p;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
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
        // some code goes here
        // not necessary for lab1
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> mdPageList =  dbfile.insertTuple(tid, t);
        PageId pid = null;
        for(Page p : mdPageList) {
            pid = p.getId();
            p.markDirty(true, tid);
            if(!pageBuffer.containsKey(pid)) {
                if(currentSize == maxSize) {
                    evictPage();
                }
                pageBuffer.put(pid, p);
                pageIdList.add(pid);
                currentSize++;
            }

            // TODO 解决bug
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
        // some code goes here
        // not necessary for lab1
        DbFile dbfile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> mdPageList = dbfile.deleteTuple(tid, t);
        PageId pid = null;
        for(Page p : mdPageList) {
            pid = p.getId();
            p.markDirty(true, tid);
            if(!pageBuffer.containsKey(pid)) {
                if(currentSize == maxSize) {
                    evictPage();
                }
                pageBuffer.put(pid, p);
                pageIdList.add(pid);
                currentSize++;
            }
            // TODO 解决bug
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        int len = pageIdList.size();
        for(int i = len; i > 0; --i) {
            PageId pid = pageIdList.get(0);
            // Page p = pageBuffer.getOrDefault(pid, null);
            flushPage(pid);
        }
        /**
        while(len > 0) {
            PageId pid = pageIdList.get(len - 1);
            flushPage(pid);
            len = pageIdList.size();
        }*/
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageBuffer.remove(pid);
        pageIdList.remove(pid);
        currentSize--;
    }


    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = pageBuffer.getOrDefault(pid, null);
        if(p == null) {
            return;
        }
        DbFile df = Database.getCatalog().getDatabaseFile(p.getId().getTableId());
        df.writePage(p);
        pageBuffer.remove(pid);
        pageIdList.remove(pid);
        currentSize--;

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
        // some code goes here
        // not necessary for lab1
        /**
        for( Map.Entry e : pageBuffer.entrySet()) {
            PageId key = (PageId)e.getKey();
            pageBuffer.remove(key);
            currentSize--;
            break;
        }*/
        if(!pageIdList.isEmpty()) {
            PageId pid = pageIdList.get(0);
            pageIdList.remove(0);
            pageBuffer.remove(pid);
            currentSize--;
        }
    }

}
