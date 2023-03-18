package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfieldIndex;
    private Type gbfieldType;
    private int afieldIndex;
    private Op aop;

    private TupleDesc td;
    private Map<Field, Integer> curValues;
    private Map<Field, Integer> curSums;
    private Map<Field, Integer> curCounts;
    private StringField defaultField = new StringField("i hate java i hate java i hate java", 35);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfieldIndex = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afieldIndex = afield;
        this.aop = what;

        this.td = null;
        this.curValues = new HashMap<>();
        this.curSums = new HashMap<>();
        this.curCounts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Set the tupledesc
        if (td == null) {
            Type aFieldType = tup.getTupleDesc().getFieldType(afieldIndex);
            String aFieldName = tup.getTupleDesc().getFieldName(afieldIndex);
            if (gbfieldIndex == NO_GROUPING) {
                td = new TupleDesc(
                    new Type[]{aFieldType},
                    new String[]{aFieldName}
                );
            } else {
                Type gbFieldType = tup.getTupleDesc().getFieldType(gbfieldIndex);
                assert gbFieldType.equals(this.gbfieldType);
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
        
        IntField aField = (IntField) tup.getField(afieldIndex);
        int newVal = aField.getValue();
        int curVal = 0;
        int curSum = 0;
        int curCount = 0;

        // set current values based on group
        if (curValues.containsKey(gbfield)) {
            curVal = curValues.get(gbfield);
            curSum = curSums.get(gbfield);
            curCount = curCounts.get(gbfield);
        } else {
            // set based on operation
            switch (aop) {
                case MIN:
                    curVal = Integer.MAX_VALUE;
                    break;
                case MAX:
                    curVal = Integer.MIN_VALUE;
                    break;
                case SUM:
                case AVG:
                case COUNT:
                case SUM_COUNT:
                case SC_AVG:
                    curVal = 0;
                    break;
            }
        }
        
        // update values based on operation
        curSum += newVal;
        curCount++;
        switch (aop) {
            case MIN:
                if (newVal < curVal)
                    curVal = newVal;
                break;
            case MAX:
                if (newVal > curVal)
                    curVal = newVal;
                break;
            case SUM:
                curVal = curSum;
                break;
            case AVG:
                curVal = curSum/curCount;
                break;
            case COUNT:
                curVal = curCount;
                break;
            case SUM_COUNT:
            case SC_AVG:
                break;
        }

        // set the new values
        curValues.put(gbfield, curVal);
        curSums.put(gbfield, curSum);
        curCounts.put(gbfield, curCount);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Entry<Field, Integer> entry : curValues.entrySet()) {
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
