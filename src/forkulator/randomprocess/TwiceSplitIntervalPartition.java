package forkulator.randomprocess;

import forkulator.helpers.DistributionHelper;

import java.util.Arrays;

/**
 * Produces service times that are a sub-division of a larger
 * service time which is split twice.
 * 
 * @author stefan
 *
 */
public class TwiceSplitIntervalPartition extends IntervalPartition {
    int partDiv;
    IntervalPartition firstPartitionSplitter;
    IntervalPartition secondPartitionSplitter;


    /**
     * Constructor
     *
     * The interval is [0,size] and the boundaries are multinomial distributed.
     *
     * @param partDiv
     * @param firstPartitionSplitter
     * @param secondPartitionSplitter
     */
    public TwiceSplitIntervalPartition(int partDiv, IntervalPartition firstPartitionSplitter,
                                       IntervalPartition secondPartitionSplitter) {
        this.num_partitions = 1;
        this.size = 0;
        this.partDiv = partDiv;
        this.firstPartitionSplitter = firstPartitionSplitter;
        this.secondPartitionSplitter = secondPartitionSplitter;
        boundaries = new double[this.num_partitions + 1];
        current_sample = 0;
        this.setBoundaries();
    }

    public TwiceSplitIntervalPartition(double size, int num_partitions, int partDiv,
                                       IntervalPartition firstPartitionSplitter,
                                       IntervalPartition secondPartitionSplitter) {
        this.size = size;
        this.num_partitions = num_partitions;
        this.partDiv = partDiv;
        this.firstPartitionSplitter =
                firstPartitionSplitter;
        this.secondPartitionSplitter = secondPartitionSplitter;
        boundaries = new double[this.num_partitions + 1];
        current_sample = 0;
        this.setBoundaries();
    }

    /**
     * Creates  multinomial distributed partition boundaries.
     */
    protected void setBoundaries() {
        boundaries[0] = 0;
        boundaries[boundaries.length -1] = size;
        if (num_partitions > 1) {
            // Use partitioner.
            int numOfFirstPartitions = num_partitions / partDiv;
            IntervalPartition first = this.firstPartitionSplitter.getNewPartition(size, numOfFirstPartitions);
            for (int i = 0; i < numOfFirstPartitions; i++) {
                double partitionSize = first.nextInterval();
                IntervalPartition second =
                        this.secondPartitionSplitter.getNewPartition(partitionSize, partDiv);
                for (int j = 0; j < partDiv; j++)
                    this.boundaries[i * partDiv + j + 1] =
                            this.boundaries[i * partDiv + j] + second.nextInterval();
            }
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
        return new TwiceSplitIntervalPartition(size, num_partitions, this.partDiv,
                this.firstPartitionSplitter, this.secondPartitionSplitter);
    }
    
}
