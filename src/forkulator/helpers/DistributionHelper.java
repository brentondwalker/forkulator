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
        int numPartitions = 16;
//        IntervalPartition tmpPartition = new TwiceSplitIntervalPartition(2,
//                new UniformRandomIntervalPartition(1,1),
//                new ConstantIntervalPartition(1,1));
        IntervalPartition tmpPartition = new ExponentialRandomIntervalPartition(1,16, 0.7);
        for (int c = 0; c < 1;c++) {
            double[] probabilities = new double[numPartitions];
            Arrays.fill(probabilities, 1d / numPartitions);
    //        MultinomialDistribution distribution =
    //                new MultinomialDistribution(10*numPartitions, probabilities);
//            IntervalPartition intervalPartition = new MultinomialIntervalPartition(1.0, numPartitions);
            IntervalPartition intervalPartition = tmpPartition.getNewPartition(10, numPartitions);
            double sum = 0;
            for (int i = 0; i < numPartitions; i++) {
                probabilities[i] = intervalPartition.nextInterval();
                sum += probabilities[i];
            }
            System.out.println(Arrays.toString(probabilities));
            System.out.println(sum);
        }
    }
}
