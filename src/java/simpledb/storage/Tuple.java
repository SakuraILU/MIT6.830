package simpledb.storage;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    TupleDesc tupleDesc;
    private Field fields[];
    RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *           the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *          index of the field to change. It must be a valid index.
     * @param f
     *          new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *          field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= fields.length)
            throw new NoSuchElementException("i is not a valid field reference");

        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String str = "";
        for (int i = 0; i < fields.length - 1; ++i) {
            str += (fields[i].toString() + '\t');
        }
        str += fields[fields.length - 1].toString();

        return str;
        // throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *         An iterator which iterates over all the fields of this tuple
     */
    private class TupleIterator implements Iterator<Field> {
        int nextPOs = 0;

        public TupleIterator(int n) {
            nextPOs = n;
        }

        @Override
        public boolean hasNext() {
            return nextPOs < fields.length;
        }

        @Override
        public Field next() {
            return fields[nextPOs++];
        }

    }

    public Iterator<Field> fields() {
        // some code goes here
        return new TupleIterator(0);
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
    }

    public static Tuple merge(Tuple tuple1, Tuple tuple2) {
        Tuple tuple = new Tuple(TupleDesc.merge(tuple1.getTupleDesc(), tuple2.getTupleDesc()));
        System.arraycopy(tuple1.fields, 0, tuple.fields, 0, tuple1.fields.length);
        System.arraycopy(tuple2.fields, 0, tuple.fields, tuple1.fields.length, tuple2.fields.length);
        return tuple;
    }
}
