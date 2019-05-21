package forkulator.randomprocess;

/**
 * CorrelatedExponentialSumIntertimeProcess
 * 
 * An IntertimeProcess that produces batches of correlated exponential RVs with
 * a controllable level of correlation.
 * 
 * This version produces samples that are sums of the correlated exponentials.
 * If you take the sum of i.i.d. exponential RVs you get an Erlang, but if the
 * exponentials are correlated, you get something different.  
 * 
 * XXX - this should be refactored and done in a more general way that would
 *       let us do this with any inter-time distribution.  It is convenient for
 *       comparing the ideal job division to other job divisions when we don't
 *       know the exact distribution of the resulting job sizes.
 * 
 * NOTE: the correlation rho that is passed is not the correlation that is
 * actually produced.  For rho in [0,1] it is close.  For rho in [-1,0)
 * this method will not generally even work.
 * 
 * @author brenton
 *
 */
public class CorrelatedExponentialSumIntertimeProcess extends IntertimeProcess {

    public double rate = 1.0;
    public int batch_size = 1;
    public double rho = 0.0;
    CorrelatedExponentialIntertimeProcess ceip = null;
    
    /**
     * Constructor
     * 
     * @param rate  the rate of the exponential
     * @param k     the number of correlated samples per batch
     * @param rho   the desired correlation (the correlation achieved is a little off)
     */
    public CorrelatedExponentialSumIntertimeProcess(double rate, int k, double rho) {
        this.rate = rate;
        this.batch_size = k;
        this.rho = rho;
    
        // don't re-implement everything
        ceip = new CorrelatedExponentialIntertimeProcess(rate,k,rho);
    }

    
    /**
     * Get the next sample.
     */
    public double nextInterval() {
        double sum = 0.0;
        for (int i=0; i<batch_size; i++) {
            sum += ceip.nextInterval();
        }
        return (sum/batch_size);
    }
    
    public double nextInterval(double unused) {
        return nextInterval();
    }
    
    public double nextInterval(int unused) {
        return nextInterval();
    }
    
    /**
     * XXX
     */
    @Override
    public IntertimeProcess clone() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * XXX
     */
    @Override
    public String processParameters() {
        // TODO Auto-generated method stub
        return null;
    }

    
    /**
     * This just generates samples for analysis.
     * 
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: CorrelatedExponentialSumIntertimeProcess k cor num_samples\n");
            System.exit(0);
        }
        int k = Integer.parseInt(args[0]);
        if (k<2) {
            System.err.println("ERROR: k must be greater or equal to 2");
            System.exit(0);
        }
        double cor = Double.parseDouble(args[1]);
        if (cor < -1.0 || cor > 1.0) {
            System.err.println("ERROR: cor must be between -1 and +1");
            System.exit(0);
        }
        int num_samples = Integer.parseInt(args[2]);
        CorrelatedExponentialSumIntertimeProcess cip = new CorrelatedExponentialSumIntertimeProcess(1, k, cor);
        for (int i=0; i<num_samples; i++) {
            System.out.println(""+cip.nextInterval());
        }
    }
}
