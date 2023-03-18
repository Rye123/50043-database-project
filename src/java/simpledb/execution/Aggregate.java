package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afieldIndex;
    private int gfieldIndex;
    private Op aop;
    private Aggregator argh;

    private OpIterator arghIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afieldIndex = afield;
        this.gfieldIndex = gfield;
        this.aop = aop;

        Type afieldType = child.getTupleDesc().getFieldType(afieldIndex);
        if (afieldType == Type.INT_TYPE)
            this.argh = new IntegerAggregator(gfield, Type.INT_TYPE, afield, aop);
        else
            this.argh = new StringAggregator(gfield, Type.STRING_TYPE, afield, aop);
        this.arghIterator = argh.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (gfieldIndex == Aggregator.NO_GROUPING)
            return null;
        return child.getTupleDesc().getFieldName(gfieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            Tuple tup = child.next();
            argh.mergeTupleIntoGroup(tup);
        }
        arghIterator = argh.iterator();
        arghIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        assert arghIterator != null;
        if (arghIterator.hasNext())
            return arghIterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        arghIterator = argh.iterator();
        arghIterator.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        Type aFieldType = child.getTupleDesc().getFieldType(afieldIndex);
        String aFieldName = aop.toString() + "(" + child.getTupleDesc().getFieldName(afieldIndex) + ")"; // give an iNfOrMaTiVe nAmE
        TupleDesc td = null;
        if (gfieldIndex == -1) {
            td = new TupleDesc(
                new Type[]{aFieldType},
                new String[]{aFieldName}
            );
        } else {
            Type gbFieldType = child.getTupleDesc().getFieldType(gfieldIndex);
            String gbFieldName = child.getTupleDesc().getFieldName(gfieldIndex);
            td = new TupleDesc(
                new Type[]{gbFieldType, aFieldType},
                new String[]{gbFieldName, aFieldName}
            );
        }
        return td;
    }

    public void close() {
        super.close();
        child.close();
        arghIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0])
            this.child = children[0];
    }

}
