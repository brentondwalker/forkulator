package forkulator.randomprocess;

import java.util.Arrays;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.CorrelatedRandomVectorGenerator;
import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;

/**
 * CorrelatedExponentialIntertimeProcess
 * 
 * An IntertimeProcess that produces batches of correlated exponential RVs with
 * a controllable level of correlation.
 * 
 * NOTE: the correlation rho that is passed is not the correlation that is
 * actually produced.  For rho in [0,1] it is close.  For rho in [-1,0)
 * this method will not generally even work.
 * 
 * @author brenton
 *
 */
public class CorrelatedExponentialIntertimeProcess extends IntertimeProcess {

	public double rate = 1.0;
	public int batch_size = 1;
	public double rho = 0.0;
	private Array2DRowRealMatrix covariance_matrix = null;
	GaussianRandomGenerator gnrg = new GaussianRandomGenerator(new JDKRandomGenerator());
	CorrelatedRandomVectorGenerator crvg = null;
	NormalDistribution normal = new NormalDistribution();
	private double[] samples = null;
	private int sample_index = 0;
	
	/**
	 * Constructor
	 * 
	 * @param rate	the rate of the exponential
	 * @param k		the number of correlated samples per batch
	 * @param rho	the desired correlation (the correlation achieved is a little off)
	 */
	public CorrelatedExponentialIntertimeProcess(double rate, int k, double rho) {
		this.rate = rate;
		this.batch_size = k;
		this.rho = rho;
		this.covariance_matrix = new Array2DRowRealMatrix(batch_size,batch_size);
		for (int i=0; i<batch_size; i++) {
			for (int j=0; j<batch_size; j++) {
				if (i==j) {
					covariance_matrix.setEntry(i, j, 1.0);
				} else {
					covariance_matrix.setEntry(i, j, rho);
				}
			}
		}
		crvg = new CorrelatedRandomVectorGenerator(covariance_matrix, 0.00001, gnrg);
	}

	
	/**
	 * Generate a new batch of samples.
	 * The samples can be accessed through nextInterval().
	 * 
	 * Samples are correlated within a batch, with no correlation
	 * between batches.  The samples you get after calling this
	 * method will be independent of any samples you got before calling it.
	 */
    public void generateSamples() {
        samples = crvg.nextVector();
        for (int j=0; j<batch_size; j++) {
            samples[j] = normal.cumulativeProbability(samples[j]);
        }
        for (int j=0; j<batch_size; j++) {
            samples[j] = -Math.log(1.0 - samples[j])/rate;
        }
    }

    
    /**
     * Get the next sample.
     */
	public double nextInterval(int unused) {
	    if ((sample_index % batch_size) == 0) {
	        sample_index = 0;
	        generateSamples();
	    }
	    return (rate * samples[sample_index++]);
	}
	
	
	/**
	 * XXX
	 */
	@Override
	public IntertimeProcess clone() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * XXX
	 */
	@Override
	public String processParameters() {
		// TODO Auto-generated method stub
		return null;
	}

	
	/**
	 * This just generates samples for analysis.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
	    if (args.length != 3) {
	        System.err.println("usage: CorrelatedExponentialIntertimeProcess k cor num_samples\n");
	        System.exit(0);
	    }
	    int k = Integer.parseInt(args[0]);
	    if (k<2) {
	        System.err.println("ERROR: k must be greater or equal to 2");
	        System.exit(0);
	    }
	    double cor = Double.parseDouble(args[1]);
	    if (cor < -1.0 || cor > 1.0) {
	        System.err.println("ERROR: cor must be between -1 and +1");
            System.exit(0);
	    }
	    int num_samples = Integer.parseInt(args[2]);
        CorrelatedExponentialIntertimeProcess cip = new CorrelatedExponentialIntertimeProcess(1, k, cor);
	    for (int i=0; i<num_samples; i++) {
	        System.out.println(""+cip.nextInterval()+"\t"+cip.nextInterval());
	    }
	}
}
