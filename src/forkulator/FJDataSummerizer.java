package forkulator;

import java.io.*;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import java.lang.Math;

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
public class FJDataSummerizer extends FJBaseDataAggregator implements Serializable {

	/**
	 * Supposed to add this to say the class implements Serializable.
	 */
	private static final long serialVersionUID = 1L;

	private static final int STABILITY_SOJOURN_THRESHOLD = 10;

	// Number of samples in current batch
	private int samples_in_batch = 0;

	// structures to hold results at the end of the experiment
	private double binwidth = 0.1;
	public double[] job_sojourn_d = null;
	public double[] job_waiting_d = null;
	public double[] job_lt_waiting_d = null;
	public double[] job_lasttask_d = null;
	public double[] job_service_d = null;
	public double[] job_inorder_sojourn_d = null;
	public double[] job_cputime_d = null;

	public int maxSojournTimeIncreasing = 0;
	public int sojournTimeIncreasing = 0;

	// optionally this object can contain a FJPathLogger
	public FJPathLogger path_logger = null;

	public String[] params = null;

	/**
	 * Constructor
	 *
	 * @param max_samples
	 */
	public FJDataSummerizer(int max_samples) {
		super(max_samples, 1);
	}

	/**
	 * Constructor
	 *
	 * @param max_samples
	 * @param batch_size
	 */
	public FJDataSummerizer(int max_samples, int batch_size) {
		super(max_samples, batch_size);
//		int numOfSamples = max_samples/batch_size;
		job_sojourn_d = new double[max_samples];
		job_waiting_d = new double[max_samples];
		job_lasttask_d = new double[max_samples];
		job_service_d = new double[max_samples];
		job_inorder_sojourn_d = new double[max_samples];
		job_cputime_d = new double[max_samples];
		job_lt_waiting_d = new double[max_samples];
	}

	/**
	 * Constructor
	 *
	 * @param max_samples
	 * @param batch_size
	 */
	public FJDataSummerizer(int max_samples, int batch_size, String[] params) {
		super(max_samples, batch_size);
		this.params = params;
//		int numOfSamples = max_samples/batch_size;
		job_sojourn_d = new double[max_samples];
		job_waiting_d = new double[max_samples];
		job_lasttask_d = new double[max_samples];
		job_service_d = new double[max_samples];
		job_inorder_sojourn_d = new double[max_samples];
		job_cputime_d = new double[max_samples];
		job_lt_waiting_d = new double[max_samples];
	}
	
	
	/**
	 * Grab the data we want from this job.
	 * 
	 * @param job
	 */
	public void sample(FJJob job) {
		if (job.sample) {
			if (samples_in_batch >= batch_size) {
				samples_in_batch = 0;
				if (num_samples > 1) {
					if (job_sojourn_d[num_samples - 1] > job_sojourn_d[num_samples - 2])
						sojournTimeIncreasing++;
					else {
						sojournTimeIncreasing = 0;
					}
					maxSojournTimeIncreasing = Math.max(maxSojournTimeIncreasing,
							sojournTimeIncreasing);
				}

				num_samples++;
			}
			double jst = job.tasks[0].start_time;
			double jlt = job.tasks[0].start_time;
			for (FJTask task : job.tasks) {
				jst = Math.min(jst, task.start_time);
				jlt = Math.max(jlt, task.start_time);
			}
			double sojourn_time = job.departure_time - job.arrival_time;
			double arrival_time = jst - job.arrival_time;
			double job_lt_waiting_time = jlt - job.arrival_time;
			double job_lasttask = job.departure_time - job.arrival_time;
			double job_service = job.departure_time - job.arrival_time;
			double job_inorder_sojourn = job.departure_time - job.arrival_time;
			double cputime = job.departure_time - job.arrival_time;
			if (samples_in_batch == 0) {
				job_sojourn_d[num_samples] = sojourn_time;
				job_waiting_d[num_samples] = arrival_time;
				job_lt_waiting_d[num_samples] = job_lt_waiting_time;
				job_lasttask_d[num_samples] = job_lasttask;
				job_service_d[num_samples] = job_service;
				job_inorder_sojourn_d[num_samples] = job_inorder_sojourn;
				job_cputime_d[num_samples] = cputime;
				samples_in_batch++;
			} else {
				samples_in_batch++;
				// Calculate means
				job_sojourn_d[num_samples] =
						job_sojourn_d[num_samples] + (sojourn_time-job_sojourn_d[num_samples])/samples_in_batch;
				job_waiting_d[num_samples] =
						job_waiting_d[num_samples] + (sojourn_time-job_waiting_d[num_samples])/samples_in_batch;
				job_lt_waiting_d[num_samples] =
						job_lt_waiting_d[num_samples] + (sojourn_time-job_lt_waiting_d[num_samples])/samples_in_batch;
				job_lasttask_d[num_samples] =
						job_lasttask_d[num_samples] + (sojourn_time-job_lasttask_d[num_samples])/samples_in_batch;
				job_service_d[num_samples] =
						job_service_d[num_samples] + (sojourn_time-job_service_d[num_samples])/samples_in_batch;
				job_inorder_sojourn_d[num_samples] =
						job_inorder_sojourn_d[num_samples] + (sojourn_time-job_inorder_sojourn_d[num_samples])/samples_in_batch;
				job_cputime_d[num_samples] =
						job_cputime_d[num_samples] + (sojourn_time-job_cputime_d[num_samples])/samples_in_batch;
			}
		}
		
		if (this.path_logger != null) {
			path_logger.recordJob(job);
		}
	}

	@Override
	public void appendDataAggregator(FJBaseDataAggregator dataAggregator) {
		if (dataAggregator instanceof FJDataSummerizer) {
			FJDataSummerizer aggregator = (FJDataSummerizer) dataAggregator;
			for (int i = 0; i < aggregator.num_samples; i++) {
				if (num_samples >= max_samples) {
					System.err.println("ERROR: Not enough free values in data aggregator to merge.");
					return;
				}
				job_sojourn_d[num_samples] = aggregator.job_sojourn_d[i];
				job_waiting_d[num_samples] = aggregator.job_waiting_d[i];
				job_lt_waiting_d[num_samples] = aggregator.job_lt_waiting_d[i];
				job_lasttask_d[num_samples] = aggregator.job_lasttask_d[i];
				job_service_d[num_samples] = aggregator.job_service_d[i];
				job_inorder_sojourn_d[num_samples] = aggregator.job_inorder_sojourn_d[i];
				job_cputime_d[num_samples] = aggregator.job_cputime_d[i];
				num_samples++;
			}
		}
	}

	/**
	 * Compute and print out the pdf and cdfs of sojourn time and the other stats for jobs
	 * 
	 * @param outfile_base
	 * @param binwidth
	 */
	public void printExperimentDistributions(String outfile_base, double binwidth) {
		if (job_sojourn_d == null || binwidth != this.binwidth)
			computeExperimentDistributions(binwidth);

		// print out the distributions
		// plot filename using 2:(log($3)) with lines title "sojourn", filename using 2:(log($4)) with lines title "waiting", filename using 2:(log($5)) with lines title "service"
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_dist.dat"));
            double sojourn_cdf = 0.0;
            double inorder_sojourn_cdf = 0.0;
			double waiting_cdf = 0.0;
			double lt_waiting_cdf = 0.0;
            double lasttask_cdf = 0.0;
			double service_cdf = 0.0;
			double cputime_cdf = 0.0;
			int total = num_samples;
			for (int i=0; i<job_sojourn_d.length; i++) {
                sojourn_cdf += (1.0*job_sojourn_d[i])/total;
                inorder_sojourn_cdf += (1.0*job_sojourn_d[i])/total;
                waiting_cdf += (1.0*job_waiting_d[i])/total;
				lt_waiting_cdf += (1.0*job_lt_waiting_d[i])/total;
                lasttask_cdf += (1.0*job_lasttask_d[i])/total;
				service_cdf += (1.0*job_service_d[i])/total;
				cputime_cdf += (1.0*job_cputime_d[i])/total;
				writer.write(i
						+"\t"+(i*binwidth)
                        +"\t"+(1.0*job_sojourn_d[i])/(total*binwidth)
                        +"\t"+sojourn_cdf
						+"\t"+(1.0*job_waiting_d[i])/(total*binwidth)
						+"\t"+(1.0*job_lt_waiting_d[i])/(total*binwidth)
						+"\t"+waiting_cdf
						+"\t"+(1.0*job_service_d[i])/(total*binwidth)
						+"\t"+service_cdf
						+"\t"+(1.0*job_cputime_d[i])/(total*binwidth)
                        +"\t"+cputime_cdf
                        +"\t"+(1.0*job_lasttask_d[i])/(total*binwidth)
                        +"\t"+lasttask_cdf
                        +"\t"+(1.0*job_inorder_sojourn_d[i])/(total*binwidth)
                        +"\t"+inorder_sojourn_cdf
						+"\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// Close the writer regardless of what happens...
				writer.close();
			} catch (Exception e) {
			}
		}
	}
	
	
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

	@Override
	protected void computeExperimentDistributions(double binwidth) {

	}

    @Override
	FJBaseDataAggregator getNewInstance(int max_samples, int batch_size) {
		return new FJDataSummerizer(max_samples, batch_size, params);
	}

	public boolean isUnstable() {
	    return (maxSojournTimeIncreasing >= STABILITY_SOJOURN_THRESHOLD) || cancelled;
    }
}
