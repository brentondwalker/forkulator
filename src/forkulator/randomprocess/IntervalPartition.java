package forkulator.randomprocess;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Children of this abstract class model different ways of dividing an interval.
 * 
 * @author brenton
 *
 */
public abstract class IntervalPartition extends IntertimeProcess {
    
    protected Random rand = ThreadLocalRandom.current();
    
    protected int num_partitions;
    
    protected double size;
    
    protected double boundaries[];
    
    protected int current_sample = 0;
    
    protected boolean independent_samples = false;
    
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
     * In some cases we want to get the service time distribution produced by dividing
     * jobs into tasks, but we still want the tasks to be i.i.d.  We use this method to
     * indicate that.
     * XXX: The use of IntervalPartition should be completely revised.  It should have
     *      a reference to its corresponding service process and we should only pass
     *      one service process to the FJRandomPartitionJob constructor.
     * 
     * @return
     */
    public boolean independentSamples() {
        return this.independent_samples;
    }
    
    /**
     * Get a new IntervalPartition of the same type as this one, but possibly
     * with different size and num_partitions parameters.
     * 
     * @param size
     * @param num_partitions
     * @return
     */
    public abstract IntervalPartition getNewPartition(double size, int num_partitions);
    
}
