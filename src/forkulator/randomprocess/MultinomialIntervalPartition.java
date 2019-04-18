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
    }

    /**
     * Creates  multinomial distributed partition boundaries.
     */
    protected void setBoundaries() {
        if (num_partitions == 1) {
            boundaries[0] = 0;
            boundaries[1] = size;
        } else {
            int numOfExp = 10 * num_partitions;
            double[] probabilities = new double[num_partitions];
            Arrays.fill(probabilities, 1d / num_partitions);
            int[] partitionSizes = DistributionHelper.multinomial(numOfExp, probabilities);
            for (int i = 0; i < partitionSizes.length; i++) {
                if (i > 0) {
                    double bound = ((double) partitionSizes[i - 1]) / numOfExp * size;
                    boundaries[i] = bound + boundaries[i - 1];
                } else {
                    boundaries[i] = 0;
                }
            }
            boundaries[num_partitions] = size;
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
