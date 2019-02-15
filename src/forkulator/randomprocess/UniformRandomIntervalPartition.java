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
public class UniformRandomIntervalPartition extends IntervalPartition {
    
    
    /**
     * Constructor
     * 
     * The interval is [0,size], and it will be divided by (num_partitions-1)
     * boundaries placed uniformly randomly.
     * 
     * @param size
     * @param num_partitions
     */
    public UniformRandomIntervalPartition(double size, int num_partitions) {
        this.num_partitions = num_partitions;
        this.size = size;
        boundaries = new double[this.num_partitions + 1];
        setBoundaries();
        current_sample = 0;
        //System.out.println("partitioned [0,"+size+"] : "+" "+Arrays.toString(boundaries));
    }

    /**
     * Pick (num_partitons-1) uniformly random partition boundaries
     * in the interval [0,size], and then sort them.
     */
    protected void setBoundaries() {
        boundaries[0] = 0.0;
        boundaries[num_partitions] = size;
        for (int i=1; i<num_partitions; i++) {
            boundaries[i] = rand.nextDouble() * size;
        }
        Arrays.sort(boundaries);
    }
    
    /**
     * Return the next sub-interval in the partition.
     * 
     * Should we repartition the interval when all the sub-intervals
     * have been given out, or just loop around again?
     */
    public double nextInterval() {
        double intvl = boundaries[current_sample + 1] - boundaries[current_sample];
        current_sample = (current_sample + 1) % num_partitions;
        return intvl;
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
    public IntervalPartition getNewPartiton(double size, int num_partitons) {
        return new UniformRandomIntervalPartition(size, num_partitons);
    }
    
}
