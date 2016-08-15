package forkulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
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
 * the data suring the experiment.
 * 
 * @author brenton
 *
 */
public class FJDataAggregator {
	
	// the maximum number of samples we will aggregate
	public int max_samples = 0;
	
	// the current number of samples
	public int num_samples = 0;
	
	// arrays to hold the various data we collect
	public long job_id[] = null;
	public double job_arrival_time[] = null;
	public double job_start_time[] = null;
	public double job_completion_time[] = null;
	public double job_departure_time[] = null;
	
	// structures to hold results at the end of the experiment
	double binwidth = 0.1;
	int[] job_sojourn_d = null;
	int[] job_waiting_d = null;
	int[] job_service_d = null;

	
	
	/**
	 * Constructor
	 * 
	 * @param max_samples
	 */
	public FJDataAggregator(int max_samples) {
		this.max_samples = max_samples;
		job_id = new long[max_samples];
		job_arrival_time = new double[max_samples];
		job_start_time = new double[max_samples];
		job_completion_time = new double[max_samples];
		job_departure_time = new double[max_samples];
	}
	
	
	/**
	 * Grab the data we want from this job.
	 * 
	 * @param job
	 */
	public void sample(FJJob job) {
		job_id[num_samples] = job.ID;
		job_arrival_time[num_samples] = job.arrival_time;
		double jst = job.tasks[0].start_time;
		for (FJTask task : job.tasks) {
			jst = Math.min(jst, task.start_time);
		}
		job_start_time[num_samples] = jst;
		job_completion_time[num_samples] = job.completion_time;
		job_departure_time[num_samples] = job.departure_time;
		num_samples++;
	}
	
	/**
	 * 
	 * @param binwidth
	 */
	public void computeExperimentDistributions(double binwidth) {
		this.binwidth = binwidth;
		double max_value = 0.0;
		for (int i=0; i<num_samples; i++) {
			max_value = Math.max(max_value, job_departure_time[i] - job_arrival_time[i]);
		}
		
		// initialize the distributions
		int max_bin = (int)(max_value/binwidth) + 1;
		//System.err.println("max_bin="+max_bin);
		job_sojourn_d = new int[max_bin];
		job_waiting_d = new int[max_bin];
		job_service_d = new int[max_bin];
		
		// compute the distributions
		for (int i=0; i<num_samples; i++) {
			double job_waiting_time = job_start_time[i] - job_arrival_time[i];
			double job_sojourn_time = job_departure_time[i] - job_arrival_time[i];
			double job_service_time = job_completion_time[i] - job_start_time[i];
			job_sojourn_d[(int)(job_sojourn_time/binwidth)]++;
			job_waiting_d[(int)(job_waiting_time/binwidth)]++;
			job_service_d[(int)(job_service_time/binwidth)]++;
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
			double waiting_cdf = 0.0;
			double service_cdf = 0.0;
			int total = num_samples;
			for (int i=0; i<job_sojourn_d.length; i++) {
				sojourn_cdf += (1.0*job_sojourn_d[i])/total;
				waiting_cdf += (1.0*job_waiting_d[i])/total;
				service_cdf += (1.0*job_service_d[i])/total;
				writer.write(i
						+"\t"+(i*binwidth)
						+"\t"+(1.0*job_sojourn_d[i])/(total*binwidth)
						+"\t"+sojourn_cdf
						+"\t"+(1.0*job_waiting_d[i])/(total*binwidth)
						+"\t"+waiting_cdf
						+"\t"+(1.0*job_service_d[i])/(total*binwidth)
						+"\t"+service_cdf
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
	 * Save the raw sojourn, waiting and service times for jobs.
	 * I was computing and saving the binned distributions already,
	 * but having this (almost) raw data will make later analysis
	 * possible without re-running everything.
	 * 
	 * Save the file in compressed format because these will get huge.
	 * 
	 * @param outfile_base
	 */
	public void printRawJobData(String outfile_base) {
		BufferedWriter writer = null;
		try {
			GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(outfile_base+"_jobdat.dat.gz")));
			writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
			for (int i=0; i<num_samples; i++) {
				double job_waiting_time = job_start_time[i] - job_arrival_time[i];
				double job_sojourn_time = job_departure_time[i] - job_arrival_time[i];
				double job_service_time = job_completion_time[i] - job_start_time[i];
				writer.write(i
						+"\t"+job_sojourn_time
						+"\t"+job_waiting_time
						+"\t"+job_service_time+"\n");
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
	 * compute the means of sojourn, waiting, and service times over (almost) all jobs
	 * 
	 * @return
	 */
	public ArrayList<Double> experimentMeans() {
		if (job_sojourn_d == null)
			computeExperimentDistributions(this.binwidth);
		
		double sojourn_sum = 0.0;
		double waiting_sum = 0.0;
		double service_sum = 0.0;
		for (int i=0; i<num_samples; i++) {
			waiting_sum += job_start_time[i] - job_arrival_time[i];
			sojourn_sum += job_departure_time[i] - job_arrival_time[i];
			service_sum += job_completion_time[i] - job_start_time[i];
		}
		
		double total = num_samples;
		ArrayList<Double> result = new ArrayList<Double>(9 + 1);
		result.add(sojourn_sum/total);
		result.add(waiting_sum/total);
		result.add(service_sum/total);
		result.add(total * 1.0);
		
		// also throw in the 10^-6 quantiles
		//
		// make sure the distributions have been computed and are long enough
		// to compute the quantiles we're interested in
		result.add(quantile(job_sojourn_d, num_samples, 1.0e-6, binwidth));
		result.add(quantile(job_waiting_d, num_samples, 1.0e-6, binwidth));
		result.add(quantile(job_service_d, num_samples, 1.0e-6, binwidth));

		// also add 10^-3 quantiles
		result.add(quantile(job_sojourn_d, num_samples, 1.0e-3, binwidth));
		result.add(quantile(job_waiting_d, num_samples, 1.0e-3, binwidth));
		result.add(quantile(job_service_d, num_samples, 1.0e-3, binwidth));
		
		return result;
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
					System.err.println("exceeded epsilon="+epsilon+" at i="+i+"  where d[i]="+dpdf[i]);
					//return ( binwidth*(i*dpdf[i] +(i-1)*dpdf[i-1])/(1.0*dpdf[i]+dpdf[i-1]));
					return binwidth*( (i-1) + (limit - last_ccdf)/(ccdf - last_ccdf) );
				}
				last_ccdf = ccdf;
			}
			System.err.println("WARNING: never found the specified quantile!");
		}
		
		return 0.0;
	}
	
	
}
