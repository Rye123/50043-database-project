package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfieldIndex;
    private Type gbfieldType;
    private int afieldIndex;
    private Op aop;

    private TupleDesc td;
    private Map<Field, Integer> curCounts;
    private StringField defaultField = new StringField("i hate java i hate java i hate java", 35);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfieldIndex = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afieldIndex = afield;
        this.aop = what;
        if (what != Op.COUNT)
            throw new IllegalArgumentException("y u bully me");
        this.td = null;
        this.curCounts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Set the tupledesc
        if (td == null) {
            Type aFieldType = new IntField(69).getType(); // since it's always a COUNT which is an int
            String aFieldName = aop.toString() + "(" + tup.getTupleDesc().getFieldName(afieldIndex) + ")"; // give an iNfOrMaTiVe nAmE
            if (gbfieldIndex == NO_GROUPING) {
                td = new TupleDesc(
                    new Type[]{aFieldType},
                    new String[]{aFieldName}
                );
            } else {
                Type gbFieldType = tup.getTupleDesc().getFieldType(gbfieldIndex);
                String gbFieldName = tup.getTupleDesc().getFieldName(gbfieldIndex);
                td = new TupleDesc(
                    new Type[]{gbFieldType, aFieldType},
                    new String[]{gbFieldName, aFieldName}
                );
            }
        }

        Field gbfield = defaultField;
        if (gbfieldIndex != NO_GROUPING)
            gbfield = tup.getField(gbfieldIndex);
        
        int curCount = 0;
        
        // set current values based on group
        if (curCounts.containsKey(gbfield)) {
            curCount = curCounts.get(gbfield);
        }

        // update values based on operation
        curCount++;
        curCounts.put(gbfield, curCount);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Entry<Field, Integer> entry : curCounts.entrySet()) {
            Field gField = entry.getKey();
            IntField value = new IntField(entry.getValue());
            Tuple t = new Tuple(td);
            if (gbfieldIndex == NO_GROUPING) {
                t.setField(0, value);
            } else {
                t.setField(0, gField);
                t.setField(1, value);
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
        
    }

}
