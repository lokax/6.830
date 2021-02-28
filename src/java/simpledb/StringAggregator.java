package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private Object group;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(gbfield == NO_GROUPING) {
            group = new ArrayList<String>();
        } else {
            if(gbfieldtype == Type.INT_TYPE ) {
                group = new HashMap<Integer, ArrayList<String>>();
            }
            if(gbfieldtype == Type.STRING_TYPE) {
                group = new HashMap<String, ArrayList<String>>();
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield == NO_GROUPING) {
            // ArrayList<Integer> arr = (ArrayList<Integer>) group;
            // IntField f = (IntField) tup.getField(afield);
            ((ArrayList<String>) group).add(((StringField) tup.getField(afield)).getValue());
        } else {
            if(gbfieldtype == Type.INT_TYPE) {
                HashMap<Integer, ArrayList<String>> hmap = (HashMap<Integer, ArrayList<String>>) group;
                int key =((IntField) tup.getField(gbfield)).getValue();
                String value = ((StringField) tup.getField(afield)).getValue();
                boolean isContained = hmap.containsKey(key);
                if(isContained) {
                    ArrayList<String> arr = hmap.get(key);
                    arr.add(value);
                } else {
                    ArrayList<String> arr = new ArrayList<>();
                    arr.add(value);
                    hmap.put(key, arr);
                }
            } else {
                assert (gbfieldtype == Type.STRING_TYPE);
                HashMap<String, ArrayList<String>> hmap = (HashMap<String, ArrayList<String>>) group;
                String key = ((StringField) tup.getField(gbfield)).getValue();
                String  value = ((StringField) tup.getField(afield)).getValue();
                boolean isContained = hmap.containsKey(key);
                if(isContained) {
                    ArrayList<String> arr = hmap.get(key);
                    arr.add(value);
                } else {
                    ArrayList<String> arr = new ArrayList<>();
                    arr.add(value);
                    hmap.put(key, arr);
                }
            }
        }

    }


    private class StrAggrIterator implements OpIterator{
        private Iterator<Tuple> itr;
        private ArrayList<Tuple> tpArr;
        private TupleDesc td;

        public StrAggrIterator() {
            itr = null;
            tpArr = new ArrayList<>();
            td = getTupleDesc();

            Field k = null;
            Field f = null;
            if(gbfield == NO_GROUPING) {
                // Tuple t = new Tuple(td);
                Tuple t = new Tuple(td);
                f = new IntField(excuteOp((ArrayList<Integer>) group));
                t.setField(0, f);
            } else {

                for(Map.Entry e : ((HashMap<Integer, ArrayList<Integer>>) group).entrySet()) {
                    int res = excuteOp((ArrayList<Integer>) e.getValue());
                    f = new IntField(res);
                    Tuple t = new Tuple(td);
                    if(gbfieldtype == Type.INT_TYPE) {
                        // Tuple t = new Tuple(td);
                        // f = new IntField(res);
                        k = new IntField((int)e.getKey());
                        t.setField(0, k);
                        t.setField(1, f);
                        tpArr.add(t);
                        // break;
                    }

                    if(gbfieldtype == Type.STRING_TYPE) {
                        // Tuple t = new Tuple(td);
                        String gbName = (String) e.getKey();
                        k = new StringField(gbName, gbName.length());
                        // f = new IntField(res);
                        t.setField(0, k);
                        t.setField(1, f);
                        tpArr.add(t);
                        // break;
                    }
                }
            }

        }


        private int excuteOp(ArrayList<Integer> arr) {
            assert(!arr.isEmpty());
            int res = 0;
            assert(what == Op.COUNT);

            return arr.size();

        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            itr = tpArr.iterator();
        }

        @Override
        public void close() {
            itr = null;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(itr == null) {
                return false;
            }
            if(itr.hasNext()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(itr == null) {
                throw new NoSuchElementException("itr == null");
            }
            return itr.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if(gbfield == NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
            }
        }
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
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        return new StrAggrIterator();
    }

}
