package forkulator.randomprocess;

/**
 * This is like an exponential intertime process, but it allows you to add
 * some normally-distributed overhead (limited at 0).
 * This is to model the spark system where we have deserialization time and
 * scheduler delay that average 2.92887566138 and 1.45744708995 ms per task,
 * respectively.  The variance of those is very low, about 1ms.
 * Also they aren't exactly normal, of course, but I'll just model them
 * that way.  Spark ionly resolves down to the ms, and they are on
 * that order of magnitude.
 * 
 * @author brenton
 *
 */
public class ExponentialOverheadIntertimeProcess extends IntertimeProcess {

	public double rate = 1.0;
	public double overhead_mean = 0.0;
	public double overhead_variance = 0.0;
	
	public ExponentialOverheadIntertimeProcess(double rate, double overhead_mean, double overhead_variance) {
		this.rate = rate;
		this.overhead_mean = overhead_mean;
		this.overhead_variance = overhead_variance;
		System.err.println("WARNING: using service process with overhead with mean "+overhead_mean);
	}
	
	@Override
	public double nextInterval(int jobSize) {
		return -Math.log(rand.nextDouble())/rate + Math.max(0, overhead_mean + rand.nextGaussian()*overhead_variance);
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new ExponentialOverheadIntertimeProcess(rate, overhead_mean, overhead_variance);
	}

	@Override
	public String processParameters() {
		return ""+this.rate;
	}

	
}
