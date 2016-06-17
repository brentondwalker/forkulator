package forkulator;

/**
 * 
 * For service processes this model is an upper constraint on
 * the service times.  It ensures that the service times don't get
 * too long, modulo some burstiness.
 * 
 * @author brenton
 *
 */
public class LeakyBucketServiceProcess extends IntertimeProcess {

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
	 * bucket was modified.
	 */
	public double bucketLevel = 0.0;
	public double bucketTime = 0.0;

	/**
	 * For the sake of verification, keep track of what the
	 * bucketLevel was when we returned the last interval.
	 */
	public double lastBucketLevel = 0.0;
		
	/**
	 * If the feeder process gives us a service time that the
	 * bucket can't afford, we have two choices:
	 * - discard the service time and test a new one
	 * - "speed up" the server so the task finishes in exactly
	 *   the amount of time the bucket can afford.
	 */
	public boolean discardBacklog = true;	
	
	public IntertimeProcess feederProcess = null;
	
	public LeakyBucketServiceProcess(double sigma, double rho,
			IntertimeProcess feederProcess) {
		this.sigma = sigma;
		this.rho = rho;
		this.bucketLevel = sigma;
		
		this.feederProcess = feederProcess;
	}
	
	public LeakyBucketServiceProcess(double sigma, double rho,
			IntertimeProcess feederProcess, boolean discardBacklog) {
		this.sigma = sigma;
		this.rho = rho;
		this.bucketLevel = sigma;
		
		this.feederProcess = feederProcess;
		
		this.discardBacklog = discardBacklog;
	}
	
	
	@Override
	public double nextInterval(double time) {
		// get the next service time from the feeder process
		double dt = feederProcess.nextInterval();
		
		double currentBucketLevel = Math.min(sigma, bucketLevel + rho*(time-bucketTime));
		
		if (discardBacklog) {
			
			// keep trying until we get a service time which
			// the bucket is full enough to afford
			while (currentBucketLevel < dt) {
				dt = feederProcess.nextInterval();
			}

		} else {
			// If the bucket can't afford the service time we generated,
			// return the max possible service time the bucket can afford.
			if (currentBucketLevel < dt) {
				//System.err.println("bucketLevel: "+bucketLevel+"\t currentBucketLevel: "+currentBucketLevel+"\t dt: "+dt+"\t t: "+time+"\t bucketTime: "+bucketTime);
				dt = currentBucketLevel;
			}
		}
		
		bucketTime = time;
		lastBucketLevel = currentBucketLevel;
		bucketLevel = currentBucketLevel - dt;
		
		if (bucketLevel < 0) {
			System.err.println("ERROR: leaky service bucket level is negative: "+bucketLevel);
		}
		
		return dt;
	}

	@Override
	public IntertimeProcess clone() {
		return new LeakyBucketServiceProcess(sigma, rho, feederProcess.clone(), discardBacklog);
	}
	
	/**
	 * This main() is an q&d way to do some testing of the process.
	 * 
	 * @param argv
	 */
	public static void main(String args[]) {
		double sigma = Double.parseDouble(args[0]);
		double rho = Double.parseDouble(args[1]);
		double serviceRate = Double.parseDouble(args[2]);
		double arrivalRate = Double.parseDouble(args[3]);
		int n = Integer.parseInt(args[4]);
		
		boolean discardBacklog = true;
		
		IntertimeProcess arrivals = new ExponentialIntertimeProcess(arrivalRate);
		
		LeakyBucketServiceProcess lbs = new LeakyBucketServiceProcess(sigma, rho,
				new ExponentialIntertimeProcess(serviceRate), discardBacklog);
		
		double t = 0.0;
		double st = 0.0;
		
		// for the test don't make it a proper queuing system.  Just
		// the the arrivals come and assign them service times without requiring
		// that the previous task finish.
		double totalService = 0.0;
		for (int i=0; i<n; i++) {
			double nextArrival = arrivals.nextInterval();
			t += nextArrival;
			double nextService = lbs.nextInterval(t);
			totalService += nextService;
			System.out.println(i+"\t"+t+"\t"+nextArrival+"\t"+nextService+"\t"+lbs.bucketLevel+"\t"+lbs.lastBucketLevel+"\t"+totalService);
		}
		
		/*
		double nextArrival = arrivals.nextInterval();
		double nextService = 0.0;
		
		// the arrival process ends up being something other than purely exponential here
		for (int i=0; i<n; i++) {
			t += nextArrival;
			nextService = lbs.nextInterval(t);
			System.out.println(i+"\t"+t+"\t"+nextArrival+"\t"+nextService+"\t"+lbs.bucketLevel);
			nextArrival = arrivals.nextInterval();
			if (nextArrival < nextService) {
				nextArrival = nextService;
			}
		}
		*/
		
	}
	
	
	
}
