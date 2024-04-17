package forkulator.randomprocess;

public class ErlangIntertimeProcess extends IntertimeProcess {

	public double rate = 1.0;
	public int k = 1;
	public boolean scaled = false;
	
	/**
	 * Constructor
	 * 
	 * @param rate
	 * @param k
	 */
    public ErlangIntertimeProcess(double rate, int k) {
        this(rate, k, false);
    }

    /**
     * Constructor.
     * This version includes the "scaled" parameter that divides the samples by k.
     * This is convenient for cases where we are modeling the size of perfect tasks
     * of a job whose total service is Erlang.
     * 
     * @param rate
     * @param k
     * @param scaled
     */
    public ErlangIntertimeProcess(double rate, int k, boolean scaled) {
        this.rate = rate;
        this.k = k;
        this.scaled = scaled;
    }
	
	@Override
	public double nextInterval(int jobSize) {
		double p = 1.0;
		for (int i=0; i<k; i++) {
			p *= rand.nextDouble();
		}

		return (scaled) ? -Math.log(p)/rate/k : -Math.log(p)/rate;
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}
	
	@Override
	public IntertimeProcess clone() {
		return new ErlangIntertimeProcess(rate, k);
	}

	@Override
	public String processParameters() {
		return ""+this.k+"\t"+this.rate;
	}
	

}
