package forkulator.randomprocess;

/**
 * Random process that lets you stack two distributions on top of each other with a bernoulli weight.
 * Implemented this to stack a triangle distribution on top of a uniform in order
 * to simulate the start-blocking behavior of Spark BEM jobs.
 * 
 * @author brenton
 *
 */
public class LayerCakeIntertimeProcess  extends IntertimeProcess {

    public IntertimeProcess p1 = null;
    public double weight1 = 1.0;
    public IntertimeProcess p2 = null;
    public double weight2 = 1.0;
    public double total_weight = 2.0;
    
    /**
     * Constructor.
     * 
     * @param rate
     * @param param_as_mean
     */
    public LayerCakeIntertimeProcess(IntertimeProcess p1, double weight1, IntertimeProcess p2, double weight2) {
        this.p1 = p1;
        this.p2 = p2;
        this.weight1 = weight1;
        this.weight2 = weight2;
        this.total_weight = weight1 + weight2;
    }
    
        
    @Override
    public double nextInterval(int jobSize) {        
        double u = rand.nextDouble();
        return (u*total_weight < weight1) ? p1.nextInterval() : p2.nextInterval();
    }

    @Override
    public double nextInterval(double time) {
        return nextInterval(1);
    }

    @Override
    public IntertimeProcess clone() {
        return new LayerCakeIntertimeProcess(p1, weight2, p2, weight2);
    }

    @Override
    public String processParameters() {
        return ""+this.p1+"\t"+this.weight1+"\t"+this.p2+"\t"+this.weight2;
    }

    
    public static void main(String[] args) {
        int num_samples = 10000;
        if (args.length > 0) {
            num_samples = Integer.parseInt(args[0]);
        }
        //IntertimeProcess p = new LayerCakeIntertimeProcess(new UniformIntertimeProcess(2.0, 5.0), 1.0, new TriangleIntertimeProcess(2.0, 5.0, 4.0), 0.5);
        IntertimeProcess p = new LayerCakeIntertimeProcess(new UniformIntertimeProcess(0.0, 1000.0), 1.0, new TriangleIntertimeProcess(0, 1000.0, 0.0), 1.0);
        //IntertimeProcess p = new TriangleIntertimeProcess(2.0, 5.0, 4.0);
        for (int i=0; i<num_samples; i++) {
            System.out.println(p.nextInterval());
        }
    }
    
}
