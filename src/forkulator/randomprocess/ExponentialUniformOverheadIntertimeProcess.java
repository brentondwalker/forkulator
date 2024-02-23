package forkulator.randomprocess;

/**
 * This is like an exponential intertime process, but it adds some additional
 * overhead quantity drawn from a uniform distribution.
 * 
 * This is to try and match the behavior of Barrier Execution Mode in Spark.
 * 
 * @author brenton
 *
 */
public class ExponentialUniformOverheadIntertimeProcess extends IntertimeProcess {

    public double rate = 1.0;

    /**
     * The lower and upper ends of the uniform distribution used for overhead.
     */
    public double lower = 0.0;
    public double upper = 0.0;
    
    public ExponentialUniformOverheadIntertimeProcess(double rate, double lower, double upper) {
        this.rate = rate;
        this.lower = lower;
        this.upper = upper;
        if (lower >= upper) {
            System.err.println("WARNING: lower and upper overhead bounds are reversed!  Swapping them...");
            double tmp = lower;
            lower = upper;
            upper = tmp;
        }
        System.err.println("WARNING: using exponential service process with uniform("+lower+", "+upper+") overhead");
    }
    
    @Override
    public double nextInterval(int jobSize) {
        double uniform_overhead = lower + rand.nextDouble() * (upper - lower);
        double sample = (rate == 0) ? 0.0 : -Math.log(rand.nextDouble())/rate;
        return sample + uniform_overhead;
    }

    @Override
    public double nextInterval(double time) {
        return nextInterval(1);
    }

    @Override
    public IntertimeProcess clone() {
        return new ExponentialUniformOverheadIntertimeProcess(rate, lower, upper);
    }

    @Override
    public String processParameters() {
        return ""+this.rate+"\t"+this.lower+"\t"+this.upper;
    }

    
}
