package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final String NO_GROUPING_KEY = "NO_GROUPING_KEY";

    public int gbfield;
    public Type gbfieldtype;
    public int afield;
    public Op what;
    private GBHandler gbHandler;


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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        switch (what){
            case MIN:
                gbHandler =new MinHandler();
                break;
            case MAX:
                gbHandler =new MaxHandler();
                break;
            case AVG:
                gbHandler =new AvgHandler();
                break;
            case SUM:
                gbHandler =new SumHandler();
                break;
            case COUNT:
                gbHandler =new CountHandler();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator ");
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
        String key;
        if (gbfield == NO_GROUPING){
            key = NO_GROUPING_KEY;
        }else{
            Field field = tup.getField(gbfield);
            key = field.toString();
            if (gbfieldtype != null && !(field.getType().equals(gbfieldtype))){
                throw new IllegalArgumentException("Given tuple has wrong type");
            }
        }

        gbHandler.handle(key, tup.getField(afield));
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
        Map<String,Integer> results = gbHandler.getGbResult();
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        List<Tuple> tuples = new ArrayList<>();
        if(gbfield==NO_GROUPING){
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types,names);
            for(Integer value:results.values()){
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0,new IntField(value));
                tuples.add(tuple);
            }
        }else{
            types = new Type[]{gbfieldtype,Type.INT_TYPE};
            names = new String[]{"groupVal","aggregateVal"};
            tupleDesc = new TupleDesc(types,names);
            for(Map.Entry<String,Integer> entry:results.entrySet()){
                Tuple tuple = new Tuple(tupleDesc);
                if(gbfieldtype==Type.INT_TYPE){
                    tuple.setField(0,new IntField(Integer.parseInt(entry.getKey())));
                }else{
                    tuple.setField(0,new StringField(entry.getKey(),entry.getKey().length()));
                }
                tuple.setField(1,new IntField(entry.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc,tuples);
    }

    private abstract class GBHandler{
        ConcurrentHashMap<String,Integer> gbResult;
        abstract void handle(String key,Field field);
        private GBHandler(){
            gbResult = new ConcurrentHashMap<>();
        }
        public Map<String,Integer> getGbResult(){
            return gbResult;
        }
    }
    private class CountHandler extends GBHandler {
        @Override
        public void handle(String key, Field field) {
            if(gbResult.containsKey(key)){
                gbResult.put(key,gbResult.get(key)+1);
            }else{
                gbResult.put(key,1);
            }
        }
    }
    private class SumHandler extends GBHandler{
        @Override
        public void handle(String key, Field field) {
            if(gbResult.containsKey(key)){
                gbResult.put(key,gbResult.get(key)+Integer.parseInt(field.toString()));
            }else{
                gbResult.put(key,Integer.parseInt(field.toString()));
            }
        }
    }
    private class MinHandler extends GBHandler{
        @Override
        void handle(String key, Field field) {
            int tmp = Integer.parseInt(field.toString());
            if(gbResult.containsKey(key)){
                int res = gbResult.get(key)<tmp?gbResult.get(key):tmp;
                gbResult.put(key, res);
            }else{
                gbResult.put(key,tmp);
            }
        }
    }
    private class MaxHandler extends GBHandler{
        @Override
        void handle(String key, Field field) {
            int tmp = Integer.parseInt(field.toString());
            if(gbResult.containsKey(key)){
                int res = gbResult.get(key)>tmp?gbResult.get(key):tmp;
                gbResult.put(key, res);
            }else{
                gbResult.put(key,tmp);
            }
        }
    }
    private class AvgHandler extends GBHandler{
        ConcurrentHashMap<String,Integer> sum;
        ConcurrentHashMap<String,Integer> count;
        private AvgHandler(){
            count = new ConcurrentHashMap<>();
            sum = new ConcurrentHashMap<>();
        }
        @Override
        public void handle(String key, Field field) {
            int tmp = Integer.parseInt(field.toString());
            if(gbResult.containsKey(key)){
                count.put(key,count.get(key)+1);
                sum.put(key,sum.get(key)+tmp);
            }else{
                count.put(key,1);
                sum.put(key,tmp);
            }
            gbResult.put(key,sum.get(key)/count.get(key));
        }
    }

}
