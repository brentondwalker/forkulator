package forkulator.randomprocess;

/**
 * This is like an exponential intertime process, but it adds some additional
 * overhead to model the blocking overhead experienced by tasks in Spark Barrier Execution Mode.
 * 
 * In Spark BEM, a barrier job can only start if it is offered enough workers for all of its tasks to
 * start at the same time.  When a single task finishes, the executor backend informs the driver backend,
 * and the driver backend offers the scheduler just that one executor (instead of all the available
 * executors).  The job can only start when it receives a global offer which includes all of the available
 * executors.  Global offers happen on 1s intervals with the revive timer, or when a new job arrives.
 * In our model with exponential arrivals, this means the min of a uniform(0,1) and Exp(lambda).
 * 
 * In principle, all the tasks are hit by the same revive timer and arrival events, but their
 * actual finish times are not correlated, therefore the amount of blocking overhead each task experiences
 * is /probably/ independent.  
 * 
 * In fact, the start times of the tasks will sometimes be correlated with the phase of the revive timer,
 * because the start of the job may be been triggered by that timer.
 * 
 * For now, just model this phenomenon as independent min(unif,exp) for each task.
 * 
 * @author brenton
 *
 */
public class ExponentialMUniExOverhead extends IntertimeProcess {

    public double rate = 1.0;

    /**
     * The lower and upper ends of the uniform distribution used for overhead.
     */
    public double lower = 0.0;
    public double upper = 0.0;

    /**
     * the rate at which exponential events happen (that trigger a new global offer).
     */
    public double arrival_rate = 1.0;
    
    public ExponentialMUniExOverhead(double rate, double lower, double upper, double arrival_rate) {
        this.rate = rate;
        this.lower = lower;
        this.upper = upper;
        this.arrival_rate = arrival_rate;
        if (lower >= upper) {
            System.err.println("WARNING: lower and upper overhead bounds are reversed!  Swapping them...");
            double tmp = lower;
            lower = upper;
            upper = tmp;
        }
        System.err.println("WARNING: using exponential service process with min(uniform("+lower+", "+upper+"), exp("+arrival_rate+")) overhead");
    }
    
    @Override
    public double nextInterval(int jobSize) {
        double uniform_overhead = lower + rand.nextDouble() * (upper - lower);
        double exp_overhead = (arrival_rate == 0) ? 0.0 : -Math.log(rand.nextDouble())/arrival_rate;
        double sample = (rate == 0) ? 0.0 : -Math.log(rand.nextDouble())/rate;
        return sample + Math.min(uniform_overhead, exp_overhead);
    }

    @Override
    public double nextInterval(double time) {
        return nextInterval(1);
    }

    @Override
    public IntertimeProcess clone() {
        return new ExponentialMUniExOverhead(rate, lower, upper, arrival_rate);
    }

    @Override
    public String processParameters() {
        return ""+this.rate+"\t"+this.lower+"\t"+this.upper+"\t"+this.arrival_rate;
    }

    
}
