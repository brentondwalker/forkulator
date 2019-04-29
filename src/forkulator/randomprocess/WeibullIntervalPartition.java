package forkulator.randomprocess;

import org.apache.commons.math3.distribution.WeibullDistribution;

/**
 * Produces service times that are a sub-division of a larger
 * service time which is split twice.
 * 
 * @author stefan
 *
 */
public class WeibullIntervalPartition extends IntervalPartition {
    public double shape = 0.25;   // alpha  // 1/k
    public double scale = 1.0;    // beta   // lambda
    public WeibullDistribution f = null;


    /**
     * Constructor
     *
     * The interval is [0,size] and the boundaries are multinomial distributed.
     *
     * @param partDiv
     * @param firstPartitionSplitter
     * @param secondPartitionSplitter
     */
    public WeibullIntervalPartition(double shape, double scale) {
        this.num_partitions = 1;
        this.size = 0;
        this.shape = shape;
        this.scale = scale;
        boundaries = new double[this.num_partitions + 1];
        current_sample = 0;
        this.f = new WeibullDistribution(scale, shape);
        this.setBoundaries();
    }

    public WeibullIntervalPartition(double size, int num_partitions, double shape, double scale) {
        this.size = size;
        this.num_partitions = num_partitions;
        this.shape = shape;
        this.scale = scale;
        boundaries = new double[this.num_partitions + 1];
        current_sample = 0;
        this.f = new WeibullDistribution(scale, shape);
        this.setBoundaries();
    }


    protected void setBoundaries() {
        boundaries[0] = 0.0;
        boundaries[num_partitions] = size;
        double[] partitionSizes = new double[num_partitions];
        double sum = 0.;
        for (int i=0; i<num_partitions; i++) {
            partitionSizes[i] = f.sample();
            sum += partitionSizes[i];
        }
        double factor = size / sum;
        for (int i=0; i<(num_partitions-1); i++) {
            boundaries[i+1] = (partitionSizes[i] * factor + boundaries[i]);
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
        return new WeibullIntervalPartition(size, num_partitions, this.shape, this.scale);
    }
    
}
