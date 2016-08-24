package forkulator;

import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.distribution.WeibullDistribution;


/**
 * With this we only take the shape parameter and adjust the scale
 * parameter to maintain a mean of 1.0.
 * 
 * @author brenton
 *
 */
public class WeibullIntertimeProcess extends IntertimeProcess {

	public double shape = 0.25;   // alpha  // 1/k
	public double scale = 1.0;    // beta   // lambda
	public WeibullDistribution f = null;
	
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

}

