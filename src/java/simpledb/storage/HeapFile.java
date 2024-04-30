package simpledb.storage;

import java.io.*;
import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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

    private File f;
    private TupleDesc td;
    private int maxPages;
    private int id;
    private static final int pagesize = BufferPool.getPageSize();
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        if (f == null){
            assert false;
            maxPages = 0;
        } else {
            maxPages = (int) Math.ceil(f.length() / (double) pagesize);
        }
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    @Override public Page readPage(PageId pid) {
        if (pid.getTableId() != this.id) {
            throw new RuntimeException("ur bad"); // TODO
        }
        int pageNum = pid.getPageNumber();

        byte[] bytes = new byte[pagesize];
        try (RandomAccessFile raf = new RandomAccessFile(this.f, "r")) {
            raf.seek(pageNum * pagesize);
            raf.read(bytes);
            return new HeapPage(new HeapPageId(this.id, pageNum), bytes);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Error in HeapFile.java: File not found", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error in HeapFile.java: IO error", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return maxPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    public class HeapFileIterator implements DbFileIterator{
        private int currentPageNum = 0;
        private Iterator<Tuple> currentPageIter;
        TransactionId tid;

        private Iterator<Tuple> getTupleIterator(int pageNum) throws TransactionAbortedException, DbException {
            if (pageNum >= maxPages) {
                return null;
            }

            Page page = Database.getBufferPool().getPage(tid, (new HeapPageId(id, pageNum)), Permissions.READ_ONLY);
            return ((HeapPage) page).iterator(); // TODO what if it's not
        }

        private void advanceToNext() throws TransactionAbortedException, DbException {
            while (this.currentPageNum < maxPages && !this.currentPageIter.hasNext()) {
                this.currentPageNum += 1;
                this.currentPageIter = getTupleIterator(currentPageNum);
            }
        }

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.currentPageNum = 0;
            this.currentPageIter = null;
        }

        @Override public void open() throws TransactionAbortedException, DbException {
            currentPageNum = 0;
            this.currentPageIter = getTupleIterator(0);
            this.advanceToNext();
        }

        @Override public boolean hasNext() {
            boolean result =  this.currentPageIter != null && currentPageNum < maxPages;
            return result;
        }

        @Override public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.hasNext()){
                throw new NoSuchElementException("Error in HeapFile Iterator: end of the list bozo");
            }

            assert currentPageIter.hasNext();
            Tuple result = currentPageIter.next();
            this.advanceToNext();
            return result;
        }

        @Override public void rewind() throws DbException, TransactionAbortedException {
            this.open();
            // currentPageNum = 0;
            // try {
            //     currentPageIter = ((HeapPage) Database.getBufferPool().getPage(tid, (new HeapPageId(id, currentPageNum)), Permissions.READ_ONLY)).iterator();
            // } catch (TransactionAbortedException e) {
            //     System.out.println("Error in HeapFile.java Iterator Error: " + e.getMessage());
            // } catch (DbException e) {
            //     System.out.println("Error in HeapFile.java Iterator Error: " + e.getMessage());
            // }
        }

        @Override public void close() {
            this.currentPageIter = null;
        }

    };
    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

