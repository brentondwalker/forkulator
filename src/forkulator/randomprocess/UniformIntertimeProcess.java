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
public class UniformIntertimeProcess extends IntertimeProcess {

    /**
     * The lower and upper ends of the uniform distribution used for overhead.
     */
    public double lower = 0.0;
    public double upper = 0.0;
    
    public UniformIntertimeProcess(double lower, double upper) {
        this.lower = lower;
        this.upper = upper;
        if (lower >= upper) {
            System.err.println("WARNING: lower and upper overhead bounds are reversed!  Swapping them...");
            double tmp = lower;
            lower = upper;
            upper = tmp;
        }
        //System.err.println("using uniform("+lower+", "+upper+") intertime process");
    }
    
    @Override
    public double nextInterval(int jobSize) {
        return lower + rand.nextDouble() * (upper - lower);
    }

    @Override
    public double nextInterval(double time) {
        return nextInterval(1);
    }

    @Override
    public IntertimeProcess clone() {
        return new UniformIntertimeProcess(lower, upper);
    }

    @Override
    public String processParameters() {
        return ""+this.lower+"\t"+this.upper;
    }

    
}
