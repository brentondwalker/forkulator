package forkulator.randomprocess;

import java.util.PriorityQueue;

/**
 * A normal distribution may produce negative samples, but we cannot
 * return a negative inter-event time.  The solution used here is
 * to pre-sample a large number of inter-event times, and if we get
 * a negative sample we put it before the previously-sampled one, and
 * continue the process from there.  There is still some potential error
 * if the sample go back beyond the start of the collection, but that
 * is incredibly negligible for a large number of pre-samples.
 * 
 * This should preserve the mean of the resulting intertime process.
 * You can see that by considering that the mean of the inter-times
 * over any interval is about the length of the interval divided by
 * the number of events in it.  It doesn't mater if the events were
 * jumping forward or backward.
 * But it will do weird things to the variance.  I can't even begin
 * to explain that, but it's obvious it won't be preserved when you
 * look at the shape of the distribution.
 * 
 * @author brenton
 *
 */
public class FullNormalIntertimeProcess extends IntertimeProcess {
	
	public static final int NUM_PRESAMPLES = 1000000;
	
	double last_insert_time = 0.0;
	PriorityQueue<Double> samples = new PriorityQueue<Double>();
	
	/**
	 * mu and sigma are the mean and variance
	 */
	public double mu = 0.0;
	public double sigma = 1.0;
		
	/**
	 * Constructor
	 * 
	 * @param mu
	 * @param sigma
	 */
	public FullNormalIntertimeProcess(double mu, double sigma) {
		this.mu = mu;
		this.sigma = sigma;
		
		while (samples.size() < NUM_PRESAMPLES) {
			addNextSample();
		}
	}
	
	private void addNextSample() {
		// if the last_insert_time gets so large that the precision of the
		// samples is going to get lost, re-generate the pre-sample buffer
		// starting from 0.
		// This should not affect the distribution of the inter-times.
		// Realistically we'll never hit this point.
		if (last_insert_time > 45035996273700.0) {  // leave 2 sig digits off of 2^52=4,503,599,627,370,496
			System.err.println("WARNING: FullNormalIntertimeProcess regenerating pre-sample buffer.");
			last_insert_time = 0.0;
			samples.clear();
			while (samples.size() < NUM_PRESAMPLES) {
				addNextSample();
			}
		}
		
		double x = rand.nextGaussian()*sigma + mu;
		if (samples.size() == 0) {
			while (x < 0.0) {
				x = rand.nextGaussian()*sigma + mu;
			}
			samples.add(x);
			last_insert_time = x;
		} else {
			while ((x + last_insert_time) < samples.peek()) {
				System.err.println("WARNING: had to re-sample for FullNormalIntertimeProcess.  x="+x+"  last_insert_time="+last_insert_time+"  peek="+samples.peek());
				x = rand.nextGaussian()*sigma + mu;
			}
			//if (x<0)
			//	System.err.println("ALERT: inserting negative intertime: "+x);
			samples.add(last_insert_time + x);
			last_insert_time += x;
		}
	}
	
	@Override
	public double nextInterval(int jobSize) {
		double x1 = samples.poll();
		double x2 = samples.peek();
		addNextSample();
		return (x2 - x1);
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new FullNormalIntertimeProcess(mu, sigma);
	}
	
	/**
	 * The main routine here just produces a number of samples for
	 * testing purposes.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		double mu = Double.parseDouble(args[0]);
		double sigma = Double.parseDouble(args[1]);
		int n = Integer.parseInt(args[2]);

		FullNormalIntertimeProcess f = new FullNormalIntertimeProcess(mu, sigma);
		for (int i=0; i<n; i++) {
			System.out.println(f.nextInterval());
		}
	}

	@Override
	public String processParameters() {
		return ""+this.mu+"\t"+this.sigma;
	}
}
