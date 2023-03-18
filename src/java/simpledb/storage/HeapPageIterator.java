package simpledb.storage;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
    private HeapPage heapPage; 
    private int curTuple;

    public HeapPageIterator(HeapPage page) {
        this.heapPage = page;
        this.curTuple = 0;
    }

    @Override
    public boolean hasNext() {
        while(!this.heapPage.isSlotUsed(this.curTuple)) {
            if (this.curTuple < this.heapPage.numSlots-1){
                this.curTuple++;
            }else {
                return false;
            }
        }
        return true;
    }

    @Override
    public Tuple next() {
        if(!hasNext()){
            throw new NoSuchElementException();
        }
        return heapPage.tuples[this.curTuple++];
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Unable to remove from iterator");
    }
}
