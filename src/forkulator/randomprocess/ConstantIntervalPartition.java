package forkulator.randomprocess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

/**
 * Produces service times that are a random sub-division of a larger
 * service time with uniformly random interval boundaries.
 * 
 * @author brenton
 *
 */
public class ConstantIntervalPartition extends IntervalPartition {
    
    
    /**
     * Constructor
     * 
     * The interval is [0,size], and it will be divided by (num_partitions-1)
     * boundaries placed uniformly randomly.
     * 
     * @param size
     * @param num_partitions
     */
    public ConstantIntervalPartition(double size, int num_partitions) {
        this.num_partitions = num_partitions;
        this.size = size;
        boundaries = null;
        current_sample = 0;
    }

    
    /**
     * Return the next sub-interval in the partition.
     * 
     * Should we repartition the interval when all the sub-intervals
     * have been given out, or just loop around again?
     */
    public double nextInterval() {
        return size / num_partitions;
    }

    @Override
    public IntertimeProcess clone() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String processParameters() {
        // TODO Auto-generated method stub
        return null;
    }
    

    @Override
    public IntervalPartition getNewPartition(double size, int num_partitions) {
        return new ConstantIntervalPartition(size, num_partitions);
    }
    
}
