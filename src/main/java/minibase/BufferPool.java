package minibase;

/*
BufferPool manages the reading and writing of pages into memory from disk.

Access methods call into it to retrieve pages, and it fetches pages from the appropriate location.

The BufferPool is also responsible for locking;  when a transaction fetches a page, BufferPool checks that the transaction
has the appropriate locks to read/write the page.
 */

import java.io.IOException;

public class BufferPool {

    /*
    Bytes per page, including header.
     */
    private static final int PAGE_SIZE = 4096;

    /*
    Default number of pages passed to the constructor. This is used by other classes. BufferPool should use the numPages
    argument to the constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /*
    Creates a BufferPool that caches up to numPages pages.

    @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {

    }

    public static int getPageSize() {
        return PAGE_SIZE;
    }

    /*
    Retrieve the specified page with the associated permissions.

    Will acquire a lock and may block if that lock is held by another transaction.

    The retrieved page should be looked up in the buffer pool. If it is present, it should be returned.
    If it is not present, it should be added to the buffer pool and returned.
    If there is insufficient space in the buffer pool, an page should be evicted and the new page should be added in its place.

    @param tid the ID of the transaction requesting the page
    @param pid the ID of the requested page
    @param perm the requested permissions on the page

    FIXME - I should throw TransactionAbortedException, DbException
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)  {
        // some code goes here
        return null;
    }

    public synchronized void flushAllPages() throws IOException {
    }


}
