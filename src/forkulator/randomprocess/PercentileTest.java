package forkulator.randomprocess;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

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
	double p = 1.0 - 1.0e-2;
	p *= 100.0;  // Percentile takes p values as pecentages, 0-100!
	if (args.length > 0) {
	    num_samples = Integer.parseInt(args[0]);
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
    }
}
