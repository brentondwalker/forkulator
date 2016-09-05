package forkulator;

/**
 * 
 * For arrival processes this model is a lower constraint on the
 * inter-arrival times.  It ensures that you don't get too many 
 * arrivals too close together.
 * 
 * @author brenton
 *
 */
public class LeakyBucketArrivalProcess extends IntertimeProcess {

	/**
	 * sigma and rho represent the burstiness and average rate
	 * of a arrival/service process, respectively.
	 * 
	 * In the case of arrivals constrained above:
	 * In any time interval, dt, the arrivals are less than or equal
	 * to (rho * dt) + sigma.
	 */
	public double sigma = 0.0;
	public double rho = 0.0;

	/**
	 * The bucketLevel and bucketTime are set the last time the
	 * bucket was modified.  The current 
	 */
	public double bucketLevel = 0.0;
	public double bucketTime = 0.0;
	
	/**
	 * If the feeder process gives us an inter-arrival time that the
	 * bucket can't afford, we have two choices:
	 * - discard the inter-arrival time and test a new one
	 * - "queue" the event and give the next inter-arrival time as
	 *   the next possible time that the bucket will have enough
	 *   capacity.  This corresponds to a backlogged system with
	 *   a sort of queue.
	 */
	public boolean discardBacklog = true;	

	public IntertimeProcess feederProcess = null;
	
	public LeakyBucketArrivalProcess(double sigma, double rho,
			IntertimeProcess feederProcess) {
		this.sigma = sigma;
		this.rho = rho;
		this.bucketLevel = sigma;
		
		this.feederProcess = feederProcess;
	}

	public LeakyBucketArrivalProcess(double sigma, double rho,
			IntertimeProcess feederProcess, boolean discardBacklog) {
		this.sigma = sigma;
		this.rho = rho;
		this.bucketLevel = sigma;
		
		this.feederProcess = feederProcess;
		
		this.discardBacklog = discardBacklog;
	}

	
	@Override
	public double nextInterval(int jobSize) {
		// get the next inter-arrival time from the feeder process
		double dt = feederProcess.nextInterval();
		
		if (discardBacklog) {
			// keep trying until we get an inter-arrival time at which
			// the bucket will be full enough to afford the job
			while ((bucketLevel + dt*rho) < jobSize) {
				dt = feederProcess.nextInterval();
			}
			bucketLevel = Math.min(bucketLevel + dt*rho, sigma) - jobSize;
		} else {
			// generate an inter-arrival time.  If the bucket can't afford it,
			// return the next possible time the bucket will be able to afford it.
			if ((bucketLevel + dt*rho) < jobSize) {
				dt = (jobSize - bucketLevel)/rho;
				bucketLevel = 0.0;
			} else {
				bucketLevel = Math.min(bucketLevel + dt*rho, sigma) - jobSize;
			}
		}
		
		bucketTime += dt;
		
		if (bucketLevel < 0) {
			System.err.println("ERROR: leaky bucket level is negative: "+bucketLevel);
		}
		
		return dt;
	}
	
	
	@Override
	public IntertimeProcess clone() {
		return new LeakyBucketArrivalProcess(sigma, rho, feederProcess.clone(), discardBacklog);
	}
	
	
	/**
	 * This main() is an q&d way to do some testing of the process.
	 * 
	 * @param argv
	 */
	public static void main(String args[]) {
		double sigma = Double.parseDouble(args[0]);
		double rho = Double.parseDouble(args[1]);
		double rate = Double.parseDouble(args[2]);
		int n = Integer.parseInt(args[3]);
		
		boolean discardBacklog = true;
		
		LeakyBucketArrivalProcess lbp = new LeakyBucketArrivalProcess(sigma, rho,
				new ExponentialIntertimeProcess(rate), discardBacklog);
		
		double t = 0.0;
		double dt = 0.0;
		for (int i=0; i<n; i++) {
			dt = lbp.nextInterval();
			t += dt;
			System.out.println(i+"\t"+t+"\t"+dt+"\t"+lbp.bucketLevel);
		}
		
	}

	@Override
	public String processParameters() {
		return ""+this.rho+"\t"+this.sigma+"\t"+this.feederProcess.processParameters();
	}

}
