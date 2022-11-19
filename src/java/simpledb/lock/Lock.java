package simpledb.lock;

import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

/**
 * @author wangzd
 * @create 2022-10-22 11:04
 */
public class Lock {

    private TransactionId tid;
    private LockType lockType;

    public Lock(TransactionId tid, LockType lockType) {
        this.tid = tid;
        this.lockType = lockType;
    }

    public TransactionId getTid() {
        return tid;
    }

    public void setTid(TransactionId tid) {
        this.tid = tid;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }
}
