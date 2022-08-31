package forkulator.randomprocess;

/**
 * Random process that is not random.  Always returns the same value.
 * 
 * @author brenton
 *
 */
public class ConstantIntertimeProcess  extends IntertimeProcess {

	public double rate = 1.0;
	public boolean param_as_mean = false;
	public double sampleval = 0.0;

	
	/**
	 * Constructor.
	 * 
	 * If param_as_mean=true, then all samples from this process will have the value rate.
	 * Otherwise, rate will be interpreted as the rate of the process, and all samples
	 * will have the value 1/rate.
	 * 
	 * @param rate
	 * @param param_as_mean
	 */
	public ConstantIntertimeProcess(double rate, boolean param_as_mean) {
	    this.param_as_mean = param_as_mean;
        this.rate = rate;
        if (param_as_mean) {
            sampleval = rate;
        } else {
            sampleval = 1.0/rate;
        }
    }
    
	
	/**
	 * Constructor.  
	 * By default interpret the argument as the rate of the process.
	 * 
	 * @param rate
	 */
	public ConstantIntertimeProcess(double rate) {
		this(rate, false);
	}
	
	@Override
	public double nextInterval(int jobSize) {
		return sampleval;
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new ConstantIntertimeProcess(rate, param_as_mean);
	}

	@Override
	public String processParameters() {
		return ""+this.rate;
	}

}
