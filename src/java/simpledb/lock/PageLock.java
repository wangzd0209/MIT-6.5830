package simpledb.lock;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangzd
 * @create 2022-10-22 11:50
 */
public class PageLock {
    List<Lock> locks;

    public PageLock() {
        this.locks = new ArrayList<>();
    }
}
