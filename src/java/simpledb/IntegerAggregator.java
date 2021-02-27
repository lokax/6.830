package simpledb;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private Object group;
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
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        if(gbfield == NO_GROUPING) {
            group = new ArrayList<Integer>();
        } else {
            if(gbfieldtype == Type.INT_TYPE ) {
                group = new HashMap<Integer, ArrayList<Integer>>();
            }
            if(gbfieldtype == Type.STRING_TYPE) {
                group = new HashMap<String, ArrayList<Integer>>();
            }
        }


    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield == NO_GROUPING) {
            // ArrayList<Integer> arr = (ArrayList<Integer>) group;
            // IntField f = (IntField) tup.getField(afield);
            ((ArrayList<Integer>) group).add(((IntField) tup.getField(afield)).getValue());
        } else {
            if(gbfieldtype == Type.INT_TYPE) {
                HashMap<Integer, ArrayList<Integer>> hmap = (HashMap<Integer, ArrayList<Integer>>) group;
                int key =((IntField) tup.getField(gbfield)).getValue();
                int value = ((IntField) tup.getField(afield)).getValue();
                boolean isContained = hmap.containsKey(key);
                if(isContained) {
                    ArrayList<Integer> arr = hmap.get(key);
                    arr.add(value);
                } else {
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(value);
                    hmap.put(key, arr);
                }
            } else {
                assert (gbfieldtype == Type.STRING_TYPE);
                HashMap<String, ArrayList<Integer>> hmap = (HashMap<String, ArrayList<Integer>>) group;
                String key = ((StringField) tup.getField(gbfield)).getValue();
                int value = ((IntField) tup.getField(afield)).getValue();
                boolean isContained = hmap.containsKey(key);
                if(isContained) {
                    ArrayList<Integer> arr = hmap.get(key);
                    arr.add(value);
                } else {
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(value);
                    hmap.put(key, arr);
                }
            }
        }
    }

    private class InterAggrIterator implements OpIterator{
        private Iterator<Tuple> itr;
        private ArrayList<Tuple> tpArr;
        private TupleDesc td;

        public InterAggrIterator() {
            itr = null;
            tpArr = new ArrayList<>();
            td = getTupleDesc();
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            
        }

        @Override
        public void close() {

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {

        }

        @Override
        public TupleDesc getTupleDesc() {
            if(gbfield == NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[] {Type.STRING_TYPE, Type.INT_TYPE});
            }
        }
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
        // some code goes here
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

}
