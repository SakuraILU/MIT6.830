package simpledb.storage;

import java.io.Serializable;

import javax.naming.ldap.HasControls;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    // (pid, tupleno) makes unique for every tuple in the table
    // this tuple is in which page
    private PageId pid;
    // the number of this tuple WITHIN the page
    private int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *                the pageid of the page on which the tuple resides
     * @param tupleno
     *                the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (!RecordId.class.isInstance(o))
            return false;

        RecordId other = (RecordId) o;

        return this.tupleno == other.tupleno && this.pid.equals(other.pid);
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        Integer tupleno = this.tupleno;
        int hashcode = 1;
        hashcode = hashcode * 31 + pid.hashCode();
        hashcode = hashcode * 31 + tupleno.hashCode();
        return hashcode;
    }

}
