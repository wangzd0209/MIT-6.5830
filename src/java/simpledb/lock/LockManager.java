package simpledb.lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wangzd
 * @create 2022-10-22 10:14
 */
public class LockManager {

    ConcurrentHashMap<PageId, PageLock> pageLocks;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
    }

    public PageLock getLock(PageId pageId){
        pageLocks.computeIfAbsent(pageId, k -> new PageLock());
        return pageLocks.get(pageId);
    }

    public synchronized boolean acquireLock(PageLock pageLock, Lock lock){
        List<Lock> locks = pageLock.locks;
        //当前页面锁
        if (locks.isEmpty()){
            locks.add(lock);
            return true;
        }else{
            //寻找事务锁
            for (Lock lc : locks) {
                if (lc.getTid().equals(lock.getTid())) {
                    if (lc.getLockType() == LockType.EXCLUSIVE_LOCK)
                        return true;
                    else if (lc.getLockType() == LockType.SHARED_LOCK) {
                        if (locks.size() == 1) {
                            lc.setLockType(LockType.EXCLUSIVE_LOCK);
                            return true;
                        }
                        if(lc.getLockType() == LockType.SHARED_LOCK){
                            return true;
                        }else if (lc.getLockType() == LockType.EXCLUSIVE_LOCK){
                            return false;
                        }
                    }
                }
            }
            //说明没有对应的事务锁
            if (lock.getLockType() == LockType.SHARED_LOCK && locks.get(0).getLockType() == LockType.SHARED_LOCK){
                locks.add(lock);
                return true;
            }
            return false;
        }
    }


    public synchronized void releaseLock(TransactionId tid, PageId pid) {
         List<Lock> locks = pageLocks.get(pid).locks;
         for (Lock lock : locks){
             if (lock.getTid().equals(tid)){
                 locks.remove(lock);
                 return;
             }
         }
    }


    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        List<Lock> locks = pageLocks.get(p).locks;
        if (locks.isEmpty()){
            return false;
        }
        for (Lock lock :locks){
            if(lock.getTid().equals(tid)){
                return true;
            }
        }
        return false;
    }
}
