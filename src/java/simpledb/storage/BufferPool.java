package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.lock.Lock;
import simpledb.lock.LockManager;
import simpledb.lock.LockType;
import simpledb.lock.PageLock;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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

    public int numPages;
    private ConcurrentHashMap<Integer,Page> pages;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
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

        //获取当前页面的lock
        //持有锁
        //acquire lock 返回成功与否
        //判断是否等待
        PageLock pageLock = this.lockManager.getLock(pid);
        synchronized (pageLock){
            LockType lockType;
            if (perm == Permissions.READ_ONLY){
                lockType = LockType.SHARED_LOCK;
            }else{
                lockType = LockType.EXCLUSIVE_LOCK;
            }
            long start = System.currentTimeMillis();
            long timeout = new Random().nextInt(2000);
            boolean status = false;
            if (!status){
                long now = System.currentTimeMillis();
                if (now - start > timeout) {
                    throw new TransactionAbortedException();
                }
                status = lockManager.acquireLock(pageLock, new Lock(tid, lockType));
            }
        }
        if (!pages.containsKey(pid.hashCode())){
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            while (pages.size() >= numPages){
                evictPage();
            }
            Page page = file.readPage(pid);
            pages.put(pid.hashCode(), page);

        }
        return pages.get(pid.hashCode());
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
        this.lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return this.lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit){
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            restorePages(tid);
        }
        releaseLock(tid);
    }

    private synchronized void releaseLock(TransactionId tid){
        for (Integer pageId: pages.keySet()){
            if(holdsLock(tid, pages.get(pageId).getId())){
                lockManager.releaseLock(tid, pages.get(pageId).getId());
            }
        }
    }

    private void restorePages(TransactionId tid){
        for (Integer pageId: pages.keySet()){
            Page page = pages.get(pageId);
            if (page.isDirty() == tid){
                int tableId = page.getId().getTableId();
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
                pages.put(pageId, databaseFile.readPage(page.getId()));
            }
        }
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

        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = databaseFile.insertTuple(tid, t);
        updateBufferPool(pages, tid);
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
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = databaseFile.deleteTuple(tid, t);
        updateBufferPool(pages, tid);
    }

    private void updateBufferPool(List<Page> pageList, TransactionId tid) throws DbException {
        for (Page page : pageList){
            page.markDirty(true, tid);
            if (!this.pages.containsKey(page.getId().hashCode())){
                while (this.pages.size() >= numPages){
                    evictPage();
                }
            }
            pages.put(page.getId().hashCode(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : pages.values()){
            flushPage(page.getId());
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
        pages.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = pages.get(pid.hashCode());
        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
            page.markDirty(false , null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (Integer pageId: pages.keySet()){
            Page page = pages.get(pageId);
            if (page.isDirty() == tid){
                flushPage(page.getId());
            }
            page.setBeforeImage();
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {

        Integer pageId = null;

        for (Integer n : pages.keySet()){
            Page page = pages.get(n);
            if (page.isDirty() != null)
                continue;

            if (pageId == null){
                try {
                    flushPage(page.getId());
                    discardPage(page.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
        }

        if (pageId == null){
            throw new DbException("全部都是脏页");
        }


    }

}
