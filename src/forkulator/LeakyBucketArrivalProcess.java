package forkulator;

/**
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
	 * For arrival processes this model is an upper constraint on
	 * the arrivals (default).  It can also be used as a model for
	 * service times, in which case we use it as a lower bound.
	 */
	public boolean lowerBound = false;
	
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
			IntertimeProcess feederProcess, boolean lowerBound, boolean discardBacklog) {
		this.sigma = sigma;
		this.rho = rho;
		this.bucketLevel = sigma;
		
		this.feederProcess = feederProcess;
		
		this.lowerBound = lowerBound;
		this.discardBacklog = discardBacklog;
	}

	
	@Override
	public double nextInterval(double jobSize) {
		// get the next inter-arrival time from the feeder process
		double dt = feederProcess.nextInterval();

		if (discardBacklog) {
			// keep trying until we get an inter-arrival time at which
			// the bucket will be full enough
			if (lowerBound) {
				while ((bucketLevel + dt*rho) <= jobSize) {
					dt = feederProcess.nextInterval();
				}
			} else {
				while ((bucketLevel + dt*rho) >= jobSize) {
					dt = feederProcess.nextInterval();
				}
			}
		} else {
			// generate an inter-arrival time.  If the bucket can't afford it,
			// return the next possible time the bucket will be able to afford it.
			if ((!lowerBound && ((bucketLevel + dt*rho) < jobSize))
				|| (lowerBound && ((bucketLevel + dt*rho) > jobSize))) {
				dt = (jobSize - bucketLevel)/rho;
			}
		}
		
		bucketTime += dt;
		bucketLevel += dt*rho - jobSize;
		
		return dt;
	}
	
	
	
}
