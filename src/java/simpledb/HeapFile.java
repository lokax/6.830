package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        // some code goes here
        // throw new UnsupportedOperationException("implement this");

        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgno = pid.getPageNumber();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(f, "r");
            if((pgno + 1)*BufferPool.getPageSize() > file.length()) {
                file.close();
                throw new IllegalArgumentException("pid");
            }
            file.seek((pgno * BufferPool.getPageSize()));
            byte[] data = new byte[BufferPool.getPageSize()];
            int readSize = file.read(data, 0, BufferPool.getPageSize());
            if(readSize != BufferPool.getPageSize()) {
                throw new IllegalArgumentException("readSize != pageSize");
            }
            HeapPage hp = new HeapPage((HeapPageId) pid, data);
            return hp;

        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            try{
                file.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException("file op failed");

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
        // some code goes here

        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator{
        private Iterator<Tuple> itr;
        private TransactionId tid;
        private int pgNum;
        private HeapFile hf;

        public HeapFileIterator(TransactionId tid, HeapFile hf) {
            this.tid = tid;
            pgNum = 0;
            this.hf = hf;
            // open();
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            if(pgNum >= 0 && pgNum < hf.numPages()) {
               HeapPageId pid = new HeapPageId(hf.getId(), pgNum);
               HeapPage hp  = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
               itr = hp.iterator();
               return;
            }
            throw new DbException("open itr failed");
            // Database.getBufferPool().getPage(tid, )
        }

        @Override
        public void close() {
            pgNum = 0;
            itr = null;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(itr == null) {
               return false;
            }
            if(!itr.hasNext()) {

                if(pgNum >= 0 && pgNum < hf.numPages() - 1) {
                    pgNum++;
                    open();
                    if(itr.hasNext()) {
                        return true;
                    } else {
                        return false;
                    }
                }
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(itr == null || !hasNext()) {
                throw new NoSuchElementException("next failed");
            }
            return itr.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

    };


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(tid, this);
    }

}

