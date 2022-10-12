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

    public File file;
    public TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
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
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        RandomAccessFile accessFile = null;
        try {
            accessFile = new RandomAccessFile(file, "r");

            byte[] bytes = new byte[pageSize];
            accessFile.seek((pgNo) * BufferPool.getPageSize());
            int read = accessFile.read(bytes,0,BufferPool.getPageSize());
            if (read != pageSize){
                throw new IllegalArgumentException(String.format("readPage fail"));
            }
            return new HeapPage(new HeapPageId(pid.getTableId(), pgNo), bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                accessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("readPage fail"));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        HeapPageId id = (HeapPageId) page.getId();

        byte[] pageData = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(file, "rw");
        rf.seek((long) id.getPageNumber() * BufferPool.getPageSize());
        rf.write(pageData);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = new ArrayList<>();

        for (int i = 0; i < numPages(); i++){
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (p.getNumEmptySlots() == 0)
                continue;
            p.insertTuple(t);
            pages.add(p);
            return pages;
        }
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        bw.write(emptyPageData);
        bw.close();

        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        p.insertTuple(t);
        pages.add(p);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<>();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        pages.add(p);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

    class HeapFileIterator extends AbstractDbFileIterator {

        TransactionId tid;
        HeapFile file;
        HeapPage heapPage;
        Iterator<Tuple> it = null;
        int currentPageNo = 0;

        public HeapFileIterator(TransactionId tid, HeapFile file){
            this.file = file;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, new HeapPageId(file.getId(), currentPageNo), Permissions.READ_ONLY);
            it = heapPage.iterator();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (it == null)
                return null;
            //首先要处理当前页面读完了
            if (it != null && !it.hasNext())
                it = null;
            //找到限一个页面
            while (it == null && currentPageNo + 1 < file.numPages()){
                HeapPageId nextp = new HeapPageId(file.getId(), ++currentPageNo);
                heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, nextp, Permissions.READ_ONLY);
                it = heapPage.iterator();
                if(!it.hasNext())
                    it = null;
            }
            if (it == null)
                return null;

            return it.next();
        }



        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }


        @Override
        public void close() {
            super.close();
            it = null;
        }
    }

}

