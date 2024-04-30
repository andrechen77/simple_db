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

    File f;
    TupleDesc td;
    int maxpages;
    int id;
    final int pagesize = Database.getBufferPool().getPageSize();
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
            maxpages = 0;
        } else {
            maxpages = (int) Math.ceil(f.length() / (double) this.pagesize);
        }
        id = f.getAbsoluteFile().hashCode();
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

        byte[] bytes = new byte[this.pagesize];
        try (RandomAccessFile raf = new RandomAccessFile(this.f, "r")) {
            raf.seek(pid.getPageNumber() * this.pagesize);
            int numBytesRead = raf.read(bytes);
            return new HeapPage((HeapPageId) pid, bytes);

        } catch (FileNotFoundException e) {
            System.out.println("Error in HeapFile.java: File not found: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Error in HeapFile.java: IO error: " + e.getMessage());
        }
        return null;
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
        return maxpages;
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
        private int page_number = 0;
        private Iterator<Tuple> page_iterator;
        TransactionId tid;

        private void gEtnEXtiTERATOr(){
            try {
                page_iterator = ((HeapPage) Database.getBufferPool().getPage(tid, (new HeapPageId(id, page_number)), Permissions.READ_ONLY)).iterator();
            } catch (TransactionAbortedException e) {
                System.out.println("Error in HeapFile.java Iterator Error: " + e.getMessage());
            } catch (DbException e) {
                System.out.println("Error in HeapFile.java Iterator Error: " + e.getMessage());
            }
        }

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override public void open(){
            page_number = 0;
            gEtnEXtiTERATOr();
        }
        
        @Override public boolean hasNext() {
            return (page_number < maxpages) || page_iterator.hasNext();
        }

        @Override public Tuple next() throws NoSuchElementException{
            if (!this.hasNext()){
                throw new NoSuchElementException("Error in HeapFile Iterator: end of the list bozo");
            }

            if (page_iterator.hasNext()){
                return page_iterator.next();
            }

            page_number += 1;
            gEtnEXtiTERATOr();
            return this.next();
        }

        @Override public void rewind() {
            page_number = 0;
            try {
                page_iterator = ((HeapPage) Database.getBufferPool().getPage(tid, (new HeapPageId(id, page_number)), Permissions.READ_ONLY)).iterator();
            } catch (TransactionAbortedException e) {
                System.out.println("Error in HeapFile.java Iterator Error: " + e.getMessage());
            } catch (DbException e) {
                System.out.println("Error in HeapFile.java Iterator Error: " + e.getMessage());
            }
        }

        @Override public void close() {

        }

    };
    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

