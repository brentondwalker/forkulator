package forkulator;

import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.distribution.WeibullDistribution;


/**
 * With this we only take the shape parameter and adjust the scale
 * parameter to maintain a mean of 1.0.
 * Idea to do this from https://arxiv.org/pdf/1403.5996.pdf
 * 
 * - shape of 0.25 looks heavy-tailed
 * - shape of 1.0 looks exponential
 * - shape of 4.0 looks roughly Gaussian
 * 
 * @author brenton
 *
 */
public class WeibullIntertimeProcess extends IntertimeProcess {

	public double shape = 0.25;   // alpha  // 1/k
	public double scale = 1.0;    // beta   // lambda
	public WeibullDistribution f = null;
	
	/**
	 * Constructor where you set both shape and scale.
	 * 
	 * @param shape
	 * @param scale
	 */
	public WeibullIntertimeProcess(double shape, double scale) {
		this.shape = shape;
		this.scale = scale;
		this.f = new WeibullDistribution(shape, scale);
	}
	
	/**
	 * Constructor produces weibull inter-times with scale param calculated
	 * to give expectation of 1.0 for the given shape parameter.
	 * 
	 * @param shape
	 */
	public WeibullIntertimeProcess(double shape) {
		this.shape = shape;
		this.scale = 1.0/Gamma.gamma(1.0 + 1.0/shape);
		this.f = new WeibullDistribution(shape, scale);
	}
	
	@Override
	public double nextInterval(int jobSize) {
		return f.sample();
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new WeibullIntertimeProcess(shape);
	}

	/**
	 * The main routine here just produces a number of samples for
	 * testing purposes.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		double shape = Double.parseDouble(args[0]);
		int n = Integer.parseInt(args[1]);

		WeibullIntertimeProcess f = new WeibullIntertimeProcess(shape);
		for (int i=0; i<n; i++) {
			System.out.println(f.nextInterval());
		}
	}

	@Override
	public String processParameters() {
		return ""+this.shape;
	}
}

