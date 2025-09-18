package forkulator.randomprocess;

public class BimodalExponentialIntertimeProcess extends IntertimeProcess {
    
    /**
     * Like the exponential Intertime Process, but bimodal with Bernoulli probability p.
     */
    
    public double rate1 = 1.0;
    public double rate2 = 1.0;
    public double p = 1.0;
    
    public BimodalExponentialIntertimeProcess(double rate1, double rate2, double p) {
        this.rate1 = rate1;
        this.rate2 = rate2;
        if (p < 0.0 || p > 1.0 ) {
            System .err.println("ERROR: BimodalExponentialIntertimeProcess: p must be in the interval [0,1]");
            System.exit(0);
        }
        this.p = p;
    }
    
    @Override
    public double nextInterval(int jobSize) {
        double val = 0.0;
        if (rand.nextDouble() < p) {
            val = -Math.log(rand.nextDouble())/rate2;
        } else {
            val = -Math.log(rand.nextDouble())/rate1;
        }
        return val;
    }

    @Override
    public double nextInterval(double time) {
        return nextInterval(1);
    }

    @Override
    public IntertimeProcess clone() {
        return new BimodalExponentialIntertimeProcess(rate1, rate2, p);
    }

    @Override
    public String processParameters() {
        return ""+this.rate1+"\t"+rate2+"\t"+p;
    }
}
