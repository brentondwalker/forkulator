package forkulator.randomprocess;

/**
 * Random process that returns samples from a triangle distribution with parameters (a,b,c).
 * Distribution conventions as described on the wikipedia page.
 * https://en.wikipedia.org/wiki/Triangular_distribution
 * 
 * @author brenton
 *
 */
public class TriangleIntertimeProcess  extends IntertimeProcess {

    public double a, b, c;
    
    /**
     * Constructor.
     * 
     * @param rate
     * @param param_as_mean
     */
    public TriangleIntertimeProcess(double a, double b, double c) {
        if (c > b || a > c) {
            System.err.println("ERROR: TriangleIntertimeProcess requires a <= c <= b");
            System.exit(0);
        }
        this.a = a;
        this.b = b;
        this.c = c;        
    }
    
        
    @Override
    public double nextInterval(int jobSize) {
        double u = rand.nextDouble();
        return (u < (c-a)/(b-a)) ? a + Math.sqrt( u*(b-a)*(c-a) ) : b - Math.sqrt( (1-u)*(b-a)*(b-c) );
    }

    @Override
    public double nextInterval(double time) {
        return nextInterval(1);
    }

    @Override
    public IntertimeProcess clone() {
        return new TriangleIntertimeProcess(a, b, c);
    }

    @Override
    public String processParameters() {
        return ""+this.a+"\t"+this.b+"\t"+this.c;
    }

}
