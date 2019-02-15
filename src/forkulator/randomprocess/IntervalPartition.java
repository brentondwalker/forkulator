package forkulator.randomprocess;

import java.util.Arrays;
import java.util.Random;

/**
 * Children of this abstract class model different ways of dividing an interval.
 * 
 * @author brenton
 *
 */
public abstract class IntervalPartition extends IntertimeProcess {
    
    protected static Random rand = new Random();
    
    protected int num_partitions;
    
    protected double size;
    
    protected double boundaries[];
    
    protected int current_sample = 0;
    
    /**
     * This method should be called for getting service times.
     */
    public double nextInterval() {
        throw new UnsupportedOperationException("ERROR: method not implemeted");
    }
    
    public double nextInterval(double curentTime) {
        return nextInterval();
    }
    
    /**
     * Get a new IntervalPartition of the same type as this one, but possibly
     * with different size and num_partitons parameters.
     * 
     * @param size
     * @param num_partitons
     * @return
     */
    public abstract IntervalPartition getNewPartiton(double size, int num_partitons);
    
}
