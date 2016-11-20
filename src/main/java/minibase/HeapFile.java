package java.minibase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/*
HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular order.

Tuples are stored on pages, each of which is a fixed size, and the file is simply a collection of those pages.

HeapFile works closely with HeapPage. The format of HeapPages is described in the HeapPage constructor.

@see minibase.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {


    public HeapFile(File f, TupleDesc td) {
    }

    public File getFile() {
        return null;
    }

    public int getId() {
        throw new UnsupportedOperationException("implement this");
    }

    /*
    Returns the TupleDesc of the table stored in this DbFile.

    @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        throw new UnsupportedOperationException("implement this");
    }

    public Page readPage(PageId pid) {
        return null;
    }

    public void writePage(Page page) throws IOException {

    }

    public int numPages() {
        // some code goes here
        return 0;
    }

    public DbFileIterator iterator(TransactionId tid) {
        return null;
    }

}
