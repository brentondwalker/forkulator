package forkulator.randomprocess;

import java.util.Random;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;


/**
 * This is to experiment with the percentile function in Apache Commons and figure
 * out why it gives me nonsense results.
 *
 * The answer is that Percentile takes the desired p value as a percentage 0-100.
 * I thought it was 0-1.
 * 
 * @author brenton
 *
 */
public class PercentileTest {

    /**
     * Constructor
     * 
     */
    public PercentileTest(double size, int num_partitions) {
    }    

    /**
     * Everything is done in main()
     * 
     * @param args
     */
    public static void main(String[] args) {
        int num_samples = 100000000;
        double p = (1.0 - 1.0e-2)*100.0;  // Percentile takes p values as precentages, 0-100!

        if (args.length > 0) {
            num_samples = Integer.parseInt(args[0]);
            if (args.length > 1) {
                p = Double.parseDouble(args[1]);
            }
        }

        double[] samples = new double[num_samples];
        for (int i=0; i< num_samples; i++) {
            samples[i] = Math.random();
        }

        Percentile pct = new Percentile();

        System.out.println("LEGACY:\t"+pct
                .withEstimationType(Percentile.EstimationType.LEGACY)
                .evaluate(samples, p));
        System.out.println("R_1:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_1)
                .evaluate(samples, p));
        System.out.println("R_2:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_2)
                .evaluate(samples, p));
        System.out.println("R_3:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_3)
                .evaluate(samples, p));
        System.out.println("R_4:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_4)
                .evaluate(samples, p));
        System.out.println("R_5:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_5)
                .evaluate(samples, p));
        System.out.println("R_6:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_6)
                .evaluate(samples, p));
        System.out.println("R_7:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_7)
                .evaluate(samples, p));
        System.out.println("R_8:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_8)
                .evaluate(samples, p));
        System.out.println("R_9:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_9)
                .evaluate(samples, p));

        // do the same with a normal distribution
        double[] nsamples = new double[num_samples];
        Random rr = new Random();
        for (int i=0; i< num_samples; i++) {
            nsamples[i] = rr.nextGaussian();
        }
        
        System.out.println("\n\nLEGACY:\t"+pct
                .withEstimationType(Percentile.EstimationType.LEGACY)
                .evaluate(nsamples, p));
        System.out.println("R_1:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_1)
                .evaluate(nsamples, p));
        System.out.println("R_2:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_2)
                .evaluate(nsamples, p));
        System.out.println("R_3:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_3)
                .evaluate(nsamples, p));
        System.out.println("R_4:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_4)
                .evaluate(nsamples, p));
        System.out.println("R_5:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_5)
                .evaluate(nsamples, p));
        System.out.println("R_6:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_6)
                .evaluate(nsamples, p));
        System.out.println("R_7:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_7)
                .evaluate(nsamples, p));
        System.out.println("R_8:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_8)
                .evaluate(nsamples, p));
        System.out.println("R_9:\t"+pct
                .withEstimationType(Percentile.EstimationType.R_9)
                .evaluate(nsamples, p));
        
        // do another test of several quantile computations and
        // compute the mean and variance of them
        int num_quants = 100;
        int nq_samples = 1000000;
        double[] q_samples = new double[nq_samples];
        double[] quants = new double[num_quants];
        System.out.println("\n");
        for (int i=0; i<num_quants; i++) {
            for (int j=0; j<nq_samples; j++) {
                q_samples[j] = rr.nextGaussian();
            }
            quants[i] = pct.withEstimationType(Percentile.EstimationType.R_9).evaluate(q_samples, p);
            System.out.println("\t"+quants[i]);
        }
        Mean mm = new Mean();
        Variance vv = new Variance();
        System.out.println("mean: "+mm.evaluate(quants)+"\t var: "+vv.evaluate(quants));
    }
}


