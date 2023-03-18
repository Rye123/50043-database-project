package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    // Metadata
    PageId pid;
    int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
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
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pid;
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
        // Test if they are both of type RecordId
        if (!(o instanceof RecordId)){
            System.out.print("object not RecordId");
            // If not, throw exception
            throw new IllegalArgumentException("object not RecordId");
        }
        
        RecordId recordIdObj = (RecordId) o;

        if (recordIdObj.getTupleNumber() == this.tupleno && recordIdObj.getPageId().equals(this.pid)) {
            return true;
        }else {
            return false;
        }
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
        String tupleNoAsString = Integer.toString(this.tupleno);
        String pageIdAsString = Integer.toString(this.pid.getPageNumber());
        return Integer.parseInt(pageIdAsString + tupleNoAsString);

    }

}
