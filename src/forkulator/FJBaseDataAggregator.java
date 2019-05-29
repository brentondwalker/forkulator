package forkulator;

import java.io.*;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

/**
 * It is convenient to sample the actual jobs (and tasks) as we run,
 * but we end up holding huge amounts of data in RAM, and it is steadily
 * increasing, so jobs will sometimes run out of memory after running
 * for hours.
 * 
 * This class is a more efficient way to extract just the stats we
 * want from each job as they are disposed.  If we later want to
 * compute something new, we need to modify this class to collect
 * the data during the experiment.
 * 
 * @author brenton
 *
 */
abstract public class FJBaseDataAggregator implements Serializable {

	/**
	 * Supposed to add this to say the class implements Serializable.
	 */
	protected static final long serialVersionUID = 1L;

	// the maximum number of samples we will aggregate
	protected int max_samples = 0;

	// the current number of samples
	protected int num_samples = 0;

	// The batch size
	protected int batch_size = 0;

	public boolean cancelled = false;

	// optionally this object can contain a FJPathLogger
	public FJPathLogger path_logger = null;

	/**
	 * Constructor
	 *
	 * @param max_samples
	 */
	public FJBaseDataAggregator(int max_samples) {
		this.max_samples = max_samples;
	}

	public FJBaseDataAggregator(int max_samples, int batch_size) {
		this.max_samples = max_samples/batch_size;
		this.batch_size = batch_size;
	}
	
	
	/**
	 * Grab the data we want from this job.
	 * 
	 * @param job
	 */
	abstract public void sample(FJJob job);

	abstract public void appendDataAggregator(FJBaseDataAggregator dataAggregator);
	
	
	/**
	 * Tabulate the distributions for job sojourn, waiting, and service times
	 * for the sampled jobs.
	 * This is called internally by printExperimentDistributions(), so I made it protected.
	 * 
	 * @param binwidth
	 */
	abstract protected void computeExperimentDistributions(double binwidth);
	
	
	/**
	 * Compute and print out the pdf and cdfs of sojourn time and the other stats for jobs
	 * 
	 * @param outfile_base
	 * @param binwidth
	 */
	abstract public void printExperimentDistributions(String outfile_base, double binwidth);
	
	
	/**
	 * Compute the epsilon quantile of the specified histogram/distribution.
	 * If n is too small to allow computation of the specified quantile, it 
	 * returns 0.0 and prins a warning.
	 * 
	 * @param dpdf
	 * @param n
	 * @param epsilon
	 * @return
	 */
	public static double quantile(int[] dpdf, long n, double epsilon, double binwidth) {
		if (dpdf == null) {
			System.err.println("WARNING: distribution is null");
		} else if (n < 1.0/epsilon) {
			System.err.println("WARNING: datapoints: "+n+"  required: "+(1.0/epsilon)+" for epsilon="+epsilon);
		} else {
			long ccdf = n;
			long last_ccdf = n;
			long limit = (long)(n*epsilon);
			for (int i=0; i<dpdf.length; i++) {
				ccdf -= dpdf[i];
				if (ccdf <= limit) {
					//System.err.println("exceeded epsilon="+epsilon+" at i="+i+"  where d[i]="+dpdf[i]);
					//return ( binwidth*(i*dpdf[i] +(i-1)*dpdf[i-1])/(1.0*dpdf[i]+dpdf[i-1]));
					return binwidth*( (i-1) + (limit - last_ccdf)/(ccdf - last_ccdf) );
				}
				last_ccdf = ccdf;
			}
			System.err.println("WARNING: never found the specified quantile!");
		}
		
		return 0.0;
	}

	abstract FJBaseDataAggregator getNewInstance(int max_samples, int batch_size);
	
}
