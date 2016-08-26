package forkulator;

public class NormalIntertimeProcess extends IntertimeProcess {
	
	/**
	 * mu and sigma are the mean and variance
	 */
	public double mu = 0.0;
	public double sigma = 1.0;
	
	/**
	 * We can't return a negative interval/service time, so we have two
	 * options:
	 * - when we get a negative sample round it to zero
	 * - keep re-sampling until we get a non-negative sample
	 */
	public boolean round_to_zero = false;
	
	/**
	 * Constructor
	 * 
	 * @param mu
	 * @param sigma
	 */
	public NormalIntertimeProcess(double mu, double sigma) {
		this.mu = mu;
		this.sigma = sigma;
	}
	
	/**
	 * Constructor that also sets the round_to_zero parameter.
	 * 
	 * @param mu
	 * @param sigma
	 * @param round_to_zero
	 */
	public NormalIntertimeProcess(double mu, double sigma, boolean round_to_zero) {
		this.mu = mu;
		this.sigma = sigma;
		this.round_to_zero = round_to_zero;
	}
	
	@Override
	public double nextInterval(int jobSize) {
		double x = rand.nextGaussian()*sigma + mu;
		if (round_to_zero) {
			return (x < 0.0) ? 0.0 : x;
		}
		while (x < 0.0) {
			x = rand.nextGaussian()*sigma + mu;
		}
		return x;
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new NormalIntertimeProcess(mu, sigma, round_to_zero);
	}

}
