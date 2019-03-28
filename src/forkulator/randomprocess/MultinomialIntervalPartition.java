package forkulator.randomprocess;

import forkulator.helpers.DistributionHelper;

import java.util.Arrays;

/**
 * Produces service times that are a random sub-division of a larger
 * service time with uniformly random interval boundaries.
 * 
 * @author stefan
 *
 */
public class MultinomialIntervalPartition extends IntervalPartition {


    /**
     * Constructor
     *
     * The interval is [0,size] and the boundaries are multinomial distributed.
     *
     * @param size
     * @param num_partitions
     */
    public MultinomialIntervalPartition(double size, int num_partitions) {
        this.num_partitions = num_partitions;
        this.size = size;
        boundaries = new double[this.num_partitions + 1];
        setBoundaries();
        current_sample = 0;
        //System.out.println("partitioned [0,"+size+"] : "+" "+Arrays.toString(boundaries));
    }

    /**
     * Creates  multinomial distributed partition boundaries.
     */
    protected void setBoundaries() {
        double[] probabilities = new double[num_partitions + 1];
        Arrays.fill(probabilities, 1d/num_partitions);
        int[] partitionSizes = DistributionHelper.multinomial((int)size, probabilities);
        for (int i = 0; i < partitionSizes.length; i++) {
            boundaries[i] =
                    (i > 0) ? (partitionSizes[i] + partitionSizes[i - 1]) : partitionSizes[i];
        }
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
        return new MultinomialIntervalPartition(size, num_partitions);
    }
    
}
