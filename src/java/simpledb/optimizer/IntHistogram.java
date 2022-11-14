package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

    private final int numBuckets;
    private final int min;
    private final int max;
    private final double width;
    private int[] buckets;
    private int numTuple;

    /**
     * Create a new IntHistogram.
     * total numTuple
     * value numtuple0 numTuple1 numTuple2 .... numTupleN
     * range | min, min + width | min + 2 * width | min + 3 * width | .... | max -
     * width, max |
     * index 0 1 2 .... numBuckets
     * 
     * This IntHistogram should maintain a histogram of integer values that it
     * receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through
     * the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed. For
     * example, you shouldn't
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this
     *                class for histogramming
     * @param max     The maximum integer value that will ever be passed to this
     *                class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) / (double) this.numBuckets;
        this.buckets = new int[this.numBuckets];
        this.numTuple = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * 
     * @param v Value to add to the histogram
     */
    private int valueToIndex(int v) {
        if (v < min || v > max) {
            throw new IllegalArgumentException(String.format("value %d is out of range [%d, %d]", v, min, max));
        }
        return (int) ((v - min) / width);
    }

    public void addValue(int v) {
        // some code goes here

        // add one more tuple inside valueToIndex(v) bucket
        buckets[valueToIndex(v)]++;
        numTuple++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of less operator and value
     *         Selectivity is (numTuple passed filer) / (total numTuple)
     */

    private double estimateLessThanSelectivity(int v) {
        // caller may pass v+1 or v-1 (can be max + 1, min - 1)...
        // so need to handle this two case
        if (v <= min)
            return 0.0;
        if (v > max)
            return 1.0;

        // caculate complete buckets ( on the left side of valueToIndex(v) )
        double numTupleLess = 0;
        int pos = 0;
        double low = min;
        for (; pos < valueToIndex(v); ++pos) {
            numTupleLess += buckets[pos];
            low += width;
        }

        // incomplete bucket, consider bucket is evenly distributed
        // | low v low + width |
        // |<- v - low ->|
        // Proportion: (v - low) / width
        numTupleLess += (v - low) / width * buckets[pos];

        return numTupleLess / numTuple;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     *         Selectivity is (numTuple passed filer) / (total numTuple)
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        switch (op) {
            case LESS_THAN: {
                return estimateLessThanSelectivity(v);
            }
            case LESS_THAN_OR_EQ: {
                return estimateLessThanSelectivity(v + 1);
            }
            case GREATER_THAN: {
                return 1 - estimateLessThanSelectivity(v + 1);
            }
            case GREATER_THAN_OR_EQ: {
                return 1 - estimateLessThanSelectivity(v);
            }
            case EQUALS: {
                return estimateLessThanSelectivity(v + 1) - estimateLessThanSelectivity(v);
            }
            case NOT_EQUALS: {
                return 1 - (estimateLessThanSelectivity(v + 1) - estimateLessThanSelectivity(v));
            }
        }
        return -1.0;
    }

    /**
     * @return
     *         the average selectivity of this histogram.
     * 
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String str = "";
        int low = min;
        for (int i = 0; i < numBuckets; ++i) {
            low += width;
            str += String.format("[%d, %d]: %d\t", low, low + width, buckets[i]);
        }
        return str;
    }
}
