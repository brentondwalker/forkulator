package forkulator.randomprocess;

import java.util.Arrays;

/**
 * Produces service times that are a random sub-division of a larger
 * service time with exponential random interval boundaries.
 * 
 * @author brenton
 *
 */
public class ExponentialRandomIntervalPartition extends IntervalPartition {
    private double rate = 0.;

    /**
     * Constructor
     *
     * The interval is [0,size], and it will be divided by (num_partitions-1)
     * boundaries placed uniformly randomly.
     *
     * @param size
     * @param num_partitions
     */
    public ExponentialRandomIntervalPartition(double size, int num_partitions, double rate) {
        this.num_partitions = num_partitions;
        this.size = size;
        this.rate = rate;
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
        double[] partitionSizes = new double[num_partitions];
        double sum = 0.;
        for (int i=0; i<num_partitions; i++) {
            partitionSizes[i] = -Math.log(rand.nextDouble())/rate;
            sum += partitionSizes[i];
        }
        double factor = size / sum;
        for (int i=0; i<(num_partitions-1); i++) {
            boundaries[i+1] = (partitionSizes[i] * factor + boundaries[i]);
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
    public IntervalPartition getNewPartition(double size, int num_partitions) {
        return new ExponentialRandomIntervalPartition(size, num_partitions, this.rate);
    }
    
}
