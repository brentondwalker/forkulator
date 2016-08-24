package forkulator;

public class NormalIntertimeProcess extends IntertimeProcess {

	public double mu = 0.0;
	public double sigma = 1.0;
	
	public NormalIntertimeProcess(double mu, double sigma) {
		this.mu = mu;
		this.sigma = sigma;
	}
	
	@Override
	public double nextInterval(int jobSize) {
		return rand.nextGaussian()*sigma + mu;
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new NormalIntertimeProcess(mu, sigma);
	}

}
