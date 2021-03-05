package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int width;
    private int[] boxs;
    private int nTuples;
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
    	// some code goes here

        this.buckets = buckets; // buckets mean array size of box.
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) / buckets; // 100 - 1 + 1 / 10 = 10
        this.nTuples = 0;
        if(width == 0) {
            width = 1;
            // buckets = max - min + 1;
        }
        if((max - min + 1) % buckets != 0) {
            buckets += 1;
        }
        boxs = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = (v - min) / this.width;
        boxs[index]++; // tuple size for range.
        nTuples++;
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

    	// some code goes here
        if(op.equals(Predicate.Op.EQUALS) && (v < min || v > max)) {
            return 0.0;
        }
        if(op.equals(Predicate.Op.NOT_EQUALS) && (v < min || v > max)) {
            return 1.0;
        }
        if(op.equals((Predicate.Op.GREATER_THAN)) && (v  >= max)) {
            return 0.0;
        }
        if(op.equals(Predicate.Op.LESS_THAN) && (v <= min)) {
            return 0.0;
        }
        if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ) && (v > max)) {
            return 0.0;
        }
        if(op.equals(Predicate.Op.LESS_THAN_OR_EQ) && (v < min)) {
            return 0.0;
        }
        if(op.equals((Predicate.Op.GREATER_THAN)) && (v < min)) {
            return 1.0;
        }
        if(op.equals(Predicate.Op.LESS_THAN) && (v > max)) {
            return 1.0;
        }
        if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ) && (v <= min)) {
            return 1.0;
        }
        if(op.equals(Predicate.Op.LESS_THAN_OR_EQ) && (v >= max)) {
            return 1.0;
        }

        int pos = (v - min) / this.width;
        int b_left = min + pos * this.width; // pos所在的最左边界
        double b_f = 1.0 * boxs[pos] / nTuples; //
        double b_p;
        double estimate = 0.0;
        if(op.equals(Predicate.Op.EQUALS)) {
            // equality -- algorithm
            estimate = 1.0 * boxs[pos] / this.width;
            estimate = estimate / nTuples;
            return estimate;

        } else {
            // inequality -- alg
            // double b_f = 1.0 * boxs[pos] / nTuples; //
            // double b_p;
            if(op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                for(int i = pos + 1; i < boxs.length; ++i) {
                    estimate += (1.0 * boxs[i]) / nTuples;
                }
                if(op.equals(Predicate.Op.GREATER_THAN)){
                    b_p = 1.0 * (this.width - (v - b_left) + 1)  / this.width;
                } else {
                    b_p = 1.0 * (this.width - (v - b_left))  / this.width;
                }
                // double inter = b_p * b_f;
                // estimate = estimate + b_p * b_f;
            } else if(op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
                for(int i = 0; i < pos; ++i) {
                    estimate += (1.0 * boxs[i]) / nTuples;
                }
                if(op.equals(Predicate.Op.LESS_THAN)) {
                    b_p = 1.0 * (v - b_left) / this.width;
                } else {
                    b_p = 1.0 * (v - b_left + 1) / this.width;
                }
                // estimate = estimate + b_p * b_f;
            } else {
                assert (op.equals(Predicate.Op.NOT_EQUALS));
                estimate = 1.0 * boxs[pos] / this.width;
                estimate = estimate / nTuples;
                return 1.0 - estimate;
            }

        }
        estimate = estimate + b_p * b_f;
        return estimate;
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
        // some code goes here
        return null;
    }
}
