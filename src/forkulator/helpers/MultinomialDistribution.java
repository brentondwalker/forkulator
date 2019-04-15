package forkulator.helpers;

import java.util.stream.DoubleStream;

public class MultinomialDistribution {
    private BinomialDistribution distribution = null;
    private double[] probabilities;
    private int n;

    public MultinomialDistribution(int n, double[] probabilities) {
        this.n = n;
        this.probabilities = probabilities;
        distribution = new BinomialDistribution(n, probabilities[0]);
    }

    public int[] sample() {
//        double probabillitySum = DoubleStream.of(probabilities).sum();
//        if ((probabillitySum < (1.0 - 1e-12)) || (probabillitySum > (1.0 + 1e-12)))
//            return new int[]{-1};
        int d = probabilities.length;
        int[] outcome = new int[d];
        double sum = 1.0;
        int dn = n;
        for (int i = 0; i < d-1; i++) {
            distribution.probabilityOfSuccess = probabilities[i]/sum;
            distribution.numberOfTrials = dn;
//            BinomialDistribution bin = new BinomialDistribution(dn, probabilities[i]/sum);
            outcome[i] = distribution.sample();
            dn -= outcome[i];
            sum -= probabilities[i];
        }
        if (dn > 0) {
            outcome[d-1] = dn;
        }
        return outcome;
    }
}
