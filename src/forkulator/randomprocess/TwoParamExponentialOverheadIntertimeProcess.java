package forkulator.randomprocess;

/**
 * This is like an exponential intertime process, but it implements the
 * overhead model of constant plus another smaller exponential RV.
 * These are the "task service" overhead components from the tiny-tasks
 * journal paper.
 * 
 * @author brenton
 *
 */
public class TwoParamExponentialOverheadIntertimeProcess extends IntertimeProcess {

	public double rate = 1.0;
	public double const_overhead = 0.0;
	public double exp_overhead = 0.0;
	
	public TwoParamExponentialOverheadIntertimeProcess(double rate, double const_overhead, double exp_overhead) {
		this.rate = rate;
		this.const_overhead = const_overhead;
		this.exp_overhead = exp_overhead;
		System.err.println("WARNING: using service process with overhead ("+const_overhead+", "+exp_overhead+")");
	}
	
	@Override
	public double nextInterval(int jobSize) {
	    double eoverhead = (exp_overhead == 0) ? 0.0 : -Math.log(rand.nextDouble())/exp_overhead;
	    double sample = (rate == 0) ? 0.0 : -Math.log(rand.nextDouble())/rate;
		return sample + const_overhead + eoverhead;
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new TwoParamExponentialOverheadIntertimeProcess(rate, const_overhead, exp_overhead);
	}

	@Override
	public String processParameters() {
		return ""+this.rate+"\t"+this.const_overhead+"\t"+this.exp_overhead;
	}

	
}
