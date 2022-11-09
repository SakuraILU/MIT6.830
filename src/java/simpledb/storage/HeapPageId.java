package simpledb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    // this page is in which pageNo of which table
    // tableId indicate this page belong to which table
    private int tableId;
    // pgNo is indicate this page's page offset in inside this table, from 0, 1 ...
    // file_length / page_size
    private int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here

        // a classic hashcode implementation, convert the object into an integer without
        // collision
        // if object has n elements
        // loop: hashcode = hashcode * prime + element[i].hashcode()
        Integer tableId = this.tableId;
        Integer pgNo = this.pgNo;
        int hascode = 1;
        hascode = hascode * 31 + tableId.hashCode();
        hascode = hascode * 31 + pgNo.hashCode();
        return hascode;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (!HeapPageId.class.isInstance(o))
            return false;

        HeapPageId other = (HeapPageId) o;

        return (this.tableId == other.tableId) && (this.pgNo == other.pgNo);
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk. Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
