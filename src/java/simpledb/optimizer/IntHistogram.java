package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {



    private int numBucket;
    private int min;
    private int max;
    private double width;
    private List<Bucket> buckets;
    private int ntups;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.numBucket = buckets;
        this.min = min;
        this.max = max;
        this.buckets = new ArrayList<>();
        //先计算单个桶的宽度，然后分配
        this.width = (max - min + 1.0) / buckets;
        for (int i = 0; i < numBucket; i++){
            int left = (int) Math.ceil(min + i * width);
            int right = (int) Math.ceil(min + (i + 1) * width) - 1;
            if (right < left)
                right = left;
            this.buckets.add(new Bucket(left, right));
        }
    }

    //得到桶的index
    private int getIndex(int v){
        return (int) ((v - this.min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        //先找到特定的桶
        int index = getIndex(v);
        //通过list获得返回
        buckets.get(index).addCnt();
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        Bucket bucket;
        double sum;
        int index;
        switch (op){
            case EQUALS:
                index = getIndex(v);
                if (index < 0 || index >= numBucket){
                    return 0;
                }else {
                    bucket = buckets.get(index);
                    return (bucket.getCnt() * 1.0 / bucket.getWidth()) / ntups;
                }
            case GREATER_THAN:
                index = getIndex(v);
                if (index < 0){
                    return 1.0;
                }else if (index >= numBucket){
                    return 0;
                }else {
                    bucket = buckets.get(index);
                    int right = bucket.getRight();
                    sum = (right * 1.0 - v) * bucket.getCnt() / bucket.getWidth();
                    for (int i = getIndex(v) + 1; i < this.numBucket; i++) {
                        sum += buckets.get(i).getCnt();
                    }

                    return sum / ntups;
                }
            case LESS_THAN:
                index = getIndex(v);
                if (index < 0){
                    return 0;
                }else if (index >= numBucket){
                    return 1.0;
                }else {
                    bucket = buckets.get(index);
                    int left = bucket.getLeft();
                    sum = (v * 1.0 - left) * bucket.getCnt() / bucket.getWidth();
                    for (int i = getIndex(v) - 1; i >= 0; i--) {
                        sum += buckets.get(i).getCnt();
                    }
                    return sum / ntups;
                }
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder res = new StringBuilder("|| ");
        for (Bucket bucket : buckets) {
            res.append(bucket);
            res.append(" || ");
        }
        return res.toString();
    }

    private class Bucket{
        int left;
        int right;
        int cnt;

        public Bucket(int left, int right) {
            this.left = left;
            this.right = right;
        }

        public int getLeft() {
            return left;
        }

        public void setLeft(int left) {
            this.left = left;
        }

        public int getRight() {
            return right;
        }

        public void setRight(int right) {
            this.right = right;
        }

        public int getCnt() {
            return cnt;
        }

        public void setCnt(int cnt) {
            this.cnt = cnt;
        }

        public void addCnt() {
            this.cnt++;
        }

        public int getWidth(){
            return right-left+1;
        }
    }
}
