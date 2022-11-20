package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    private final int tupleNumPerHead = 8;
    private boolean isDirty;
    private TransactionId tid;

    private long timestamp; // timestamp records when to used this page (read or write tuples)

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to:
     * <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p>
     * where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     * 
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.timestamp = System.currentTimeMillis();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     * 
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here

        // Integer division discards decimal places, it already has floor effect
        return (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each
     * tuple occupying tupleSize bytes
     * 
     * @return the number of bytes in the header of a page in a HeapFile with each
     *         tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here

        // A very important bug!!!
        // floating point division must be done to preserve the number of decimal places
        // so that the ceil operation rounds up correctly
        return (int) Math.ceil(numSlots / 8.0);
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            // note: string is also fixed... only support String_Len
            // thus we can say this td.getSize() is ununsed
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte(); // read out unused bytes and discard
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td); // malloc an new Tuple to store the data parsed
        // slotId is unique inside this page, but not unique among pages,
        // one table can used many pages...
        // But if add pid....(pid, slotId) is unique for every tuple!!
        // pid --> in which page, slotId --> in which slot within the page
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                // parse bytestream according to tupleDesc, and store this data in tuple field
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); // - numSlots *
                                                                                                 // td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should
     * be updated to reflect
     * that it is no longer stored on any page.
     * 
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        int slotId = t.getRecordId().getTupleNumber();
        if (!t.getRecordId().getPageId().equals(pid) || (slotId < 0 || slotId >= numSlots))
            throw new DbException("This tuple is not in the page");
        if (!isSlotUsed(slotId))
            throw new DbException("This slot is already empty");

        tuples[slotId] = null;
        markSlotUsed(slotId, false);

        this.timestamp = System.currentTimeMillis(); // delete (write) a tuple in this page
    }

    /**
     * Adds the specified tuple to the page; the tuple should be updated to reflect
     * that it is now stored on this page.
     * 
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     * @param t The tuple to add. note that t only has pid (insert into this page),
     *          but its tupleNo must be determined here (an empty slot)
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1

        // no empty slot
        if (getNumEmptySlots() == 0)
            throw new DbException(String.format("HeapPage [%d, %d] is full", pid.getTableId(), pid.getPageNumber()));

        for (int slotId = 0; slotId < numSlots; ++slotId) {
            if (!isSlotUsed(slotId)) {
                tuples[slotId] = t;
                // now we know it's slotId, thus malloc a recordId for this tuple
                tuples[slotId].setRecordId(new RecordId(pid, slotId));
                markSlotUsed(slotId, true);
                this.timestamp = System.currentTimeMillis(); // insert (write) a tuple on this page
                return;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        isDirty = dirty;
        this.tid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if
     * the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return (isDirty) ? tid : null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int numEmptySlots = 0;
        for (int i = 0; i < numSlots; ++i) {
            if (!isSlotUsed(i))
                ++numEmptySlots;
        }
        return numEmptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     * 
     * @param i: slotId
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // every head store 8 bitmaps( 8 slots )
        int num = i / tupleNumPerHead;
        int offset = i % tupleNumPerHead;
        return ((header[num] >> offset) & 0x1) == 1;
    }

    /**
     * Return timestamp (recent used) of this page
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     * 
     * @param i:     slotId
     * @param value: true for fill and false for clear
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        int num = i / tupleNumPerHead;
        int offset = (byte) (i % tupleNumPerHead);
        if (value)
            header[num] = (byte) (header[num] | (0x1 << offset)); // set that bit to 1
        else
            header[num] = (byte) (header[num] & (~(0x1 << offset))); // set that bit to 0
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this
     *         iterator throws an UnsupportedOperationException)
     *         (note that this iterator shouldn't return tuples in empty slots!)
     */

    private class HeapPageIterator implements Iterator<Tuple> {
        int nextPos;

        public HeapPageIterator() {
            nextPos = 0;
        }

        public boolean hasNext() {
            // jump over unused slots
            for (int i = nextPos; i < numSlots; ++i)
                if (isSlotUsed(i)) {
                    nextPos = i;
                    return true;
                }
            return false;
        }

        public Tuple next() {
            // reading tuple in this page, update timestamp
            timestamp = System.currentTimeMillis();
            return tuples[nextPos++];
        }
    }

    public Iterator<Tuple> iterator() {
        // some code goes here
        return new HeapPageIterator();
    }

}
