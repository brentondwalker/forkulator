package forkulator.helpers;

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

    public static void main(String[] args) {
        System.out.println(Arrays.toString(
                DistributionHelper.multinomial(90, new double[]{1d/3d, 1d/3d, 1d/3d})));
    }
}
