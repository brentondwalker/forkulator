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
			// generate a service time.  If the bucket can't afford it,
			// return the max possible service time the bucket can afford.
			if (currentBucketLevel < dt) {
				dt = currentBucketLevel;
			}
		}
		
		bucketTime = time;
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
		double rate = Double.parseDouble(args[2]);
		int n = Integer.parseInt(args[3]);
		
		boolean discardBacklog = false;
		
		LeakyBucketServiceProcess lbp = new LeakyBucketServiceProcess(sigma, rho,
				new ExponentialIntertimeProcess(rate), discardBacklog);
		
		double t = 0.0;
		double st = 0.0;

		double[] arrivals = new double[n];
		IntertimeProcess arrivalProcess = new ExponentialIntertimeProcess(rate);
		for (int i=0; i<n; i++) {
			t += arrivalProcess.nextInterval();
			arrivals[i] = t;
		}

		for (int i=0; i<n; i++) {
			
			
			if (t < arrivals[i]) {
				t = arrivals[i];
			} else {
				
			}
			
			st = lbp.nextInterval(t);
			System.out.println(i+"\t"+t+"\t"+st+"\t"+lbp.bucketLevel);
		}
		
	}
	
	
	
}
