package forkulator.randomprocess;

import forkulator.helpers.MultinomialDistribution;

import java.util.Arrays;

/**
 * This is a faster version of the {@link MultinomialDistribution}. To optimize the generation
 * of the multinomial distribution, the {@link org.apache.commons.math3.random.Well19937c} is only
 * initialized once and used for multiple partitioning instances.
 * The {@link org.apache.commons.math3.random.Well19937c} will be recreated if the number of
 * partitions changes.
 *
 * Produces service times that are a random sub-division of a larger
 * service time with uniformly random interval boundaries.
 * 
 * @author stefan
 *
 */
public class MultinomialIntervalPartitionSingleWell extends IntervalPartition {
    private static MultinomialDistribution distribution = null;
    private static int numOfExp = 0;

    /**
     * Constructor
     *
     * The interval is [0,size] and the boundaries are multinomial distributed.
     *
     * @param size
     * @param num_partitions
     */
    public MultinomialIntervalPartitionSingleWell(double size, int num_partitions) {
        this.num_partitions = num_partitions;
        this.size = size;
        int currNumOfExp = 10 * num_partitions;
        double[] probabilities = new double[num_partitions];
        Arrays.fill(probabilities, 1d / num_partitions);
        if(MultinomialIntervalPartitionSingleWell.distribution == null ||
                currNumOfExp != MultinomialIntervalPartitionSingleWell.numOfExp) {
            MultinomialIntervalPartitionSingleWell.numOfExp = 10 * num_partitions;
            MultinomialIntervalPartitionSingleWell.distribution =
                    new MultinomialDistribution(MultinomialIntervalPartitionSingleWell.numOfExp, probabilities);
        }
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
            int[] partitionSizes = MultinomialIntervalPartitionSingleWell.distribution.sample();
            for (int i = 0; i < partitionSizes.length; i++) {
                if (i > 0) {
                    double bound = ((double) partitionSizes[i - 1]) / MultinomialIntervalPartitionSingleWell.numOfExp * size;
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
        return new MultinomialIntervalPartitionSingleWell(size, num_partitions);
    }
    
}
