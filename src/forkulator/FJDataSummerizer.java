package forkulator;

import java.io.*;

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
	public int samples_in_batch = 0;

	// structures to hold results at the end of the experiment
	private double binwidth = 0.1;
	public double[] job_sojourn_mean = null;
	public double[] worker_idle_time_mean = null;
	// to get the variance σ² this value must be devided by the number of samples
	public double[] job_sojourn_variance = null;
	public double[] job_waiting_mean = null;
	// to get the variance σ² this value must be devided by the number of samples
	public double[] job_waiting_variance = null;
	public double[] job_lt_waiting_mean = null;
	public double[] job_lasttask_mean = null;
	public double[] job_service_mean = null;
	// to get the variance σ² this value must be devided by the number of samples
	public double[] job_service_variance = null;
	public double[] job_inorder_sojourn_mean = null;
	public double[] job_cputime_mean = null;

	public int maxSojournTimeIncreasing = 0;
	public int sojournTimeIncreasing = 0;

	// optionally this object can contain a FJPathLogger
	public FJPathLogger path_logger = null;

	public String[] params = null;

	public boolean variance_calculated = false;

	private int replication_count = 1;

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
		job_sojourn_mean = new double[max_samples];
		worker_idle_time_mean = new double[max_samples];
		job_sojourn_variance = new double[max_samples];
		job_waiting_mean = new double[max_samples];
		job_waiting_variance = new double[max_samples];
		job_lasttask_mean = new double[max_samples];
		job_service_mean = new double[max_samples];
		job_service_variance = new double[max_samples];
		job_inorder_sojourn_mean = new double[max_samples];
		job_cputime_mean = new double[max_samples];
		job_lt_waiting_mean = new double[max_samples];
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
		job_sojourn_mean = new double[max_samples];
		worker_idle_time_mean = new double[max_samples];
		job_sojourn_variance = new double[max_samples];
		job_waiting_mean = new double[max_samples];
		job_waiting_variance = new double[max_samples];
		job_lasttask_mean = new double[max_samples];
		job_service_mean = new double[max_samples];
		job_service_variance = new double[max_samples];
		job_inorder_sojourn_mean = new double[max_samples];
		job_cputime_mean = new double[max_samples];
		job_lt_waiting_mean = new double[max_samples];
	}

	private double[] extendArray(double[] oldArr, int newSize) {
		double[] newArr = new double[newSize];
		System.arraycopy(oldArr, 0, newArr, 0, oldArr.length);
		return newArr;
	}

	private void checkArrayLength() {
		if (num_samples >= job_sojourn_mean.length) {
			System.out.println("Number of samples bigger than maximum samples. Extending the arrays. Please check configuration.");
			this.max_samples = num_samples + 1;
			job_sojourn_mean = extendArray(job_sojourn_mean, this.max_samples);
			worker_idle_time_mean = extendArray(worker_idle_time_mean, this.max_samples);
			job_sojourn_variance = extendArray(job_sojourn_variance, this.max_samples);
			job_waiting_mean = extendArray(job_waiting_mean, this.max_samples);
			job_waiting_variance = extendArray(job_waiting_variance, this.max_samples);
			job_lasttask_mean = extendArray(job_lasttask_mean, this.max_samples);
			job_service_mean = extendArray(job_service_mean, this.max_samples);
			job_service_variance = extendArray(job_service_variance, this.max_samples);
			job_inorder_sojourn_mean = extendArray(job_inorder_sojourn_mean, this.max_samples);
			job_cputime_mean = extendArray(job_cputime_mean, this.max_samples);
			job_lt_waiting_mean = extendArray(job_lt_waiting_mean, this.max_samples);
		}
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
					if (job_sojourn_mean[num_samples - 1] > job_sojourn_mean[num_samples - 2])
						sojournTimeIncreasing++;
					else {
						sojournTimeIncreasing = 0;
					}
					maxSojournTimeIncreasing = Math.max(maxSojournTimeIncreasing,
							sojournTimeIncreasing);
				}
//				samples_in_batch++;
				num_samples++;
				checkArrayLength();
			}
			double jst = job.tasks[0].start_time;
			double jlt = job.tasks[0].start_time;
			for (FJTask task : job.tasks) {
				jst = Math.min(jst, task.start_time);
				jlt = Math.max(jlt, task.start_time);
			}
			double sojourn_time = job.departure_time - job.arrival_time;
			double waiting_time = jst - job.arrival_time;
			double job_lt_waiting_time = jlt - job.arrival_time;
			double job_lasttask = jlt;
			double job_service = job.completion_time - jst;
			// TODO needs to be fixed
			double job_inorder_sojourn = job.departure_time - job.arrival_time;
			double cputime = 0;
			for (FJTask t : job.tasks) {
				cputime += t.service_time;
			}
			// TODO replace with mean idle time or randomly chosen server idle time?
			double idle_time = job.workerIdleTime;
			if (samples_in_batch == 0) {
				job_sojourn_mean[num_samples] = sojourn_time;
				worker_idle_time_mean[num_samples] = idle_time;
				job_waiting_mean[num_samples] = waiting_time;
				job_lt_waiting_mean[num_samples] = job_lt_waiting_time;
				job_lasttask_mean[num_samples] = job_lasttask;
				job_service_mean[num_samples] = job_service;
				job_inorder_sojourn_mean[num_samples] = job_inorder_sojourn;
				job_cputime_mean[num_samples] = cputime;
				samples_in_batch++;
			} else {
				// Calculate means
				double old_job_sojourn_mean = job_sojourn_mean[num_samples];
				job_sojourn_mean[num_samples] =
						job_sojourn_mean[num_samples] + (sojourn_time- job_sojourn_mean[num_samples])/samples_in_batch;
				worker_idle_time_mean[num_samples] =
						worker_idle_time_mean[num_samples] + (idle_time- worker_idle_time_mean[num_samples])/samples_in_batch;
				double old_job_waiting_mean = job_waiting_mean[num_samples];
				job_waiting_mean[num_samples] =
						job_waiting_mean[num_samples] + (waiting_time- job_waiting_mean[num_samples])/samples_in_batch;
				job_lt_waiting_mean[num_samples] =
						job_lt_waiting_mean[num_samples] + (sojourn_time- job_lt_waiting_mean[num_samples])/samples_in_batch;
				job_lasttask_mean[num_samples] =
						job_lasttask_mean[num_samples] + (job_lt_waiting_time- job_lasttask_mean[num_samples])/samples_in_batch;
				double old_job_service_mean = job_service_mean[num_samples];
				job_service_mean[num_samples] =
						job_service_mean[num_samples] + (job_service- job_service_mean[num_samples])/samples_in_batch;
				job_inorder_sojourn_mean[num_samples] =
						job_inorder_sojourn_mean[num_samples] + (job_inorder_sojourn- job_inorder_sojourn_mean[num_samples])/samples_in_batch;
				job_cputime_mean[num_samples] =
						job_cputime_mean[num_samples] + (cputime- job_cputime_mean[num_samples])/samples_in_batch;

				// Calculate variance
				job_sojourn_variance[num_samples] +=
						(sojourn_time-old_job_sojourn_mean)*(sojourn_time- job_sojourn_mean[num_samples]);
				job_waiting_variance[num_samples] +=
						(waiting_time-old_job_waiting_mean)*(waiting_time- job_sojourn_mean[num_samples]);
				job_service_variance[num_samples] +=
						(job_service-old_job_service_mean)*(sojourn_time- job_service_mean[num_samples]);
				samples_in_batch++;
			}
		}

		if (this.path_logger != null) {
			path_logger.recordJob(job);
		}
	}

	@Override
	public void appendDataAggregator(FJBaseDataAggregator dataAggregator) {
		if (dataAggregator instanceof FJDataSummerizer) {
			this.calcVariance();
			FJDataSummerizer aggregator = (FJDataSummerizer) dataAggregator;
			((FJDataSummerizer) dataAggregator).calcVariance();
			for (int i = 0; i < Math.min(aggregator.num_samples, num_samples); i++) {
				if (aggregator.num_samples >= num_samples) {
					System.err.println("WARNING: Data aggregators haven't the same size so" +
							"not all samples has the same number of replications.");
				}
				job_sojourn_mean[num_samples] = aggregator.job_sojourn_mean[i];
				worker_idle_time_mean[num_samples] = aggregator.worker_idle_time_mean[i];
				job_waiting_mean[num_samples] = aggregator.job_waiting_mean[i];
				job_lt_waiting_mean[num_samples] = aggregator.job_lt_waiting_mean[i];
				job_lasttask_mean[num_samples] = aggregator.job_lasttask_mean[i];
				job_service_mean[num_samples] = aggregator.job_service_mean[i];
				job_inorder_sojourn_mean[num_samples] = aggregator.job_inorder_sojourn_mean[i];
				job_cputime_mean[num_samples] = aggregator.job_cputime_mean[i];
				num_samples++;
			}
			replication_count++;
		}
	}

	/**
	 * Compute and print out the pdf and cdfs of sojourn time and the other stats for jobs
	 * 
	 * @param outfile_base
	 * @param binwidth
	 */
	public void printExperimentDistributions(String outfile_base, double binwidth) {
		if (job_sojourn_mean == null || binwidth != this.binwidth)
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
			for (int i = 0; i< job_sojourn_mean.length; i++) {
                sojourn_cdf += (1.0* job_sojourn_mean[i])/total;
                inorder_sojourn_cdf += (1.0* job_sojourn_mean[i])/total;
                waiting_cdf += (1.0* job_waiting_mean[i])/total;
				lt_waiting_cdf += (1.0* job_lt_waiting_mean[i])/total;
                lasttask_cdf += (1.0* job_lasttask_mean[i])/total;
				service_cdf += (1.0* job_service_mean[i])/total;
				cputime_cdf += (1.0* job_cputime_mean[i])/total;
				writer.write(i
						+"\t"+(i*binwidth)
                        +"\t"+(1.0* job_sojourn_mean[i])/(total*binwidth)
                        +"\t"+sojourn_cdf
						+"\t"+(1.0* job_waiting_mean[i])/(total*binwidth)
						+"\t"+(1.0* job_lt_waiting_mean[i])/(total*binwidth)
						+"\t"+waiting_cdf
						+"\t"+(1.0* job_service_mean[i])/(total*binwidth)
						+"\t"+service_cdf
						+"\t"+(1.0* job_cputime_mean[i])/(total*binwidth)
                        +"\t"+cputime_cdf
                        +"\t"+(1.0* job_lasttask_mean[i])/(total*binwidth)
                        +"\t"+lasttask_cdf
                        +"\t"+(1.0* job_inorder_sojourn_mean[i])/(total*binwidth)
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

	/**
	 * Calculates variances by dividing the calculated variance by its number of samples
	 */
	public void calcVariance() {
		if (!variance_calculated)
			for (int i = 0; i < num_samples; i++) {
				int divider = (i == num_samples-1) ? samples_in_batch : batch_size;
				job_sojourn_variance[num_samples] /= divider;
				job_waiting_variance[num_samples] /= divider;
				job_service_variance[num_samples] /= divider;
			}
		variance_calculated = true;
	}
}
