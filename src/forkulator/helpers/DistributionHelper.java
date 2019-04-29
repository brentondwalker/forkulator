package forkulator.helpers;

import forkulator.randomprocess.*;
import org.apache.commons.math3.distribution.BinomialDistribution;

import java.util.Arrays;
import java.util.stream.DoubleStream;

public class DistributionHelper {

    /**
     * This function is a java port of the python numpy implementation numpy.random.multinomial
     * which can be found at
     * (@link https://github.com/numpy/numpy/blob/master/numpy/random/mtrand/mtrand.pyx).
     *
     *         Draw samples from a multinomial distribution.
     *         The multinomial distribution is a multivariate generalisation of the
     *         binomial distribution.  Take an experiment with one of ``p``
     *         possible outcomes.  An example of such an experiment is throwing a dice,
     *         where the outcome can be 1 through 6.  Each sample drawn from the
     *         distribution represents `n` such experiments.  Its values,
     *         ``X_i = [X_0, X_1, ..., X_p]``, represent the number of times the
     *         outcome was ``i``.
     * @param n Number of experiments
     * @param probabilities Probabilities of possible outcomes. Should sum to 1
     * @return Integer array of outcome drawn from the distribution. If probabilities doesn't sum
     *         up to 1.0 an empty array is returned.
     */
    public static int[] multinomial(int n, double[] probabilities) {
        double probabillitySum = DoubleStream.of(probabilities).sum();
        if ((probabillitySum < (1.0 - 1e-12)) || (probabillitySum > (1.0 + 1e-12)))
            return new int[]{-1};
        int d = probabilities.length;
        int[] outcome = new int[d];
        double sum = 1.0;
        int dn = n;
        for (int i = 0; i < d-1; i++) {
            BinomialDistribution bin = new BinomialDistribution(dn, probabilities[i]/sum);
            outcome[i] = bin.sample();
            dn -= outcome[i];
            sum -= probabilities[i];
        }
        if (dn > 0) {
            outcome[d-1] = dn;
        }
        return outcome;
    }

    public static int[] multinomialS(int n, double[] probabilities) {
        double probabillitySum = DoubleStream.of(probabilities).sum();
        if ((probabillitySum < (1.0 - 1e-12)) || (probabillitySum > (1.0 + 1e-12)))
            return new int[]{-1};
        int d = probabilities.length;
        int[] outcome = new int[d];
        double sum = 1.0;
        int dn = n;
        BinomialDistribution bin = new BinomialDistribution(dn, probabilities[0]/sum);
        for (int i = 0; i < d-1; i++) {
            outcome[i] = bin.sample();
            dn -= outcome[i];
            sum -= probabilities[i];
        }
        if (dn > 0) {
            outcome[d-1] = dn;
        }
        return outcome;
    }



    static double worstRuntime(double[] times, int workers) {
        Arrays.sort(times);
        return bucketRuntime(times, workers);
    }

    static double smallestRuntime(double[] times, int workers) {
        Arrays.sort(times);
        for (int i = 0; i < times.length / 2; i++) {
            double temp = times[i];
            times[i] = times[times.length - 1 - i];
            times[times.length - 1 - i] = temp;
        }
        return bucketRuntime(times, workers);
    }

    static double bucketRuntime(double[] times, int workers) {
        if (workers == 0) return 0;
        double[] buckets = new double[workers];
        double runtime = 0;
        for (int i = 0; i < times.length; i++) {
            double currSmallest = Integer.MAX_VALUE;
            int smallestIdx = 0;
            for (int j = 0; j < buckets.length; j++) {
                if (buckets[j] <= 0) {
                    smallestIdx = j;
                    break;
                } else if (buckets[j] < currSmallest) {
                    currSmallest = buckets[j];
                    smallestIdx = j;
                }
            }
            buckets[smallestIdx] += times[i];
        }
        double biggestBucket = buckets[0];
        for (int i = 1; i < buckets.length; i++) {
            if (buckets[i] > biggestBucket) {
                biggestBucket = buckets[i];
            }
        }
        return biggestBucket;
    }

    public static void main(String[] args) {
//        IntertimeProcess e = new ConstantIntertimeProcess(0.1);
//        for (int i = 0; i < 100; i++)
//            System.out.println(e.nextInterval());
//        double[] probabilities = new double[40];
//        Arrays.fill(probabilities, 1./40);
//        MultinomialDistribution dist = new MultinomialDistribution(400, probabilities);
//        System.out.println(Arrays.toString(
//                DistributionHelper.multinomial(400, probabilities)));
//        System.out.println(Arrays.toString(
//                dist.sample()));
////        for (int i = 0; i < 100000;i++) {
//            dist.sample();
//            DistributionHelper.multinomial(400, probabilities);
//            DistributionHelper.multinomialS(400, probabilities);
//            IntervalPartition intervalPartition = new MultinomialIntervalPartition(1.0, 40);
//        }
        int[] partitions = new int[]{4,8,12,16,20,24,28,32,48,64};
        int workers = 4;
//        double[] smallestRuntime = new double[partitions.length];
//        double[] normalRuntime = new double[partitions.length];
//        double[] worstRuntime = new double[partitions.length];
//        IntervalPartition tmpPartition = new TwiceSplitIntervalPartition(2,
//                new UniformRandomIntervalPartition(1,1),
//                new ConstantIntervalPartition(1,1));
        for (int partitionIdx = 0; partitionIdx < partitions.length; partitionIdx++) {
            int []distributionBuckets = new int[100];
            int numPartitions = partitions[partitionIdx];
//            IntervalPartition tmpPartition = new ConstantIntervalPartition(1,1);
            IntervalPartition tmpPartition = new WeibullIntervalPartition(1, 5);
//            IntervalPartition tmpPartition = new ExponentialRandomIntervalPartition(1,1, 0.7);
//            IntervalPartition tmpPartition = new UniformRandomIntervalPartition(1,16);
            double accSmallestRuntime = 0;
            double accNormalRuntime = 0;
            double accWorstRuntime = 0;
            int numTests = 1000000;
            for (int c = 0; c < numTests;c++) {
                double[] probabilities = new double[numPartitions];
                Arrays.fill(probabilities, 1d / numPartitions);
                //        MultinomialDistribution distribution =
                //                new MultinomialDistribution(10*numPartitions, probabilities);
//            IntervalPartition intervalPartition = new MultinomialIntervalPartition(1.0, numPartitions);
                IntervalPartition intervalPartition = tmpPartition.getNewPartition(workers,
                        numPartitions);
                double sum = 0;
//                System.out.print("|");
                for (int i = 0; i < numPartitions; i++) {
                    probabilities[i] = intervalPartition.nextInterval();
                    distributionBuckets[(int)(probabilities[i]*distributionBuckets.length/workers)]++;
//                    for (int pr = 0; pr < probabilities[i]*100; pr++)
//                        System.out.print(" ");
//                    System.out.print("|");
                    sum += probabilities[i];
                }
//                System.out.println("");
//            System.out.println(Arrays.toString(probabilities));
//            System.out.println(sum);
//            System.out.println("Smallest runtime " + DistributionHelper.smallestRuntime(probabilities, 4));
                accNormalRuntime += DistributionHelper.bucketRuntime(probabilities, workers);
                accWorstRuntime += DistributionHelper.worstRuntime(probabilities, workers);
                accSmallestRuntime += DistributionHelper.smallestRuntime(probabilities, workers);

            }
//            smallestRuntime[partitionIdx] = accSmallestRuntime/numTests;
//            normalRuntime[partitionIdx] = accNormalRuntime/numTests;
//            worstRuntime[partitionIdx] = accWorstRuntime/numTests;
            System.out.print("w5_"+numPartitions);
            for(int i = 0; i < distributionBuckets.length; i++) {
                System.out.print(" " + distributionBuckets[i]);
            }
            System.out.println("");
//            System.out.println(partitions[partitionIdx] + " " + (accSmallestRuntime/numTests) +
//                    " " + (accNormalRuntime/numTests) + " " + (accWorstRuntime/numTests));
//            System.out.println("Normal runtime " + accNormalRuntime/numTests);
//            System.out.println("Smallest runtime " + accSmallestRuntime/numTests);
//            System.out.println("Worst runtime " + accWorstRuntime/numTests);
        }
    }
}
