package forkulator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedList;

public class FJSimulator {

	public static boolean DEBUG = false;
	public static final int QUEUE_STABILITY_THRESHOLD = 10000;
	
	public LinkedList<QEvent> event_queue = new LinkedList<QEvent>();
	
	public int num_workers;
	public int num_tasks;
	public IntertimeProcess arrival_process;
	public double service_rate;
	public FJServer server = null;
	
	/**
	 * constructor
	 * 
	 * @param num_workers
	 * @param num_tasks
	 * @param arrival_rate
	 * @param service_rate
	 */
	public FJSimulator(String server_queue_type, int num_workers, int num_tasks, IntertimeProcess arrival_process, double service_rate) {
		this.num_workers = num_workers;
		this.num_tasks = num_tasks;
		this.arrival_process = arrival_process;
		this.service_rate = service_rate;
		
		if (server_queue_type.toLowerCase().equals("s")) {
			this.server = new FJSingleQueueServer(num_workers);
		} else if (server_queue_type.toLowerCase().equals("w")) {
			this.server = new FJWorkerQueueServer(num_workers);
		} else {
			System.err.println("ERROR: unknown server queue type: "+server_queue_type);
			System.exit(1);
		}
		FJServer.setSimulator(this);
	}

	
	/**
	 * put the initial events in the simulation queue and start processing them
	 * 
	 * @param num_jobs
	 */
	public void run(long num_jobs, int sampling_interval) {
		// before we generated all the job arrivals at once
		// now to save space we only have one job arrival in the queue at a time
		event_queue.add(new QJobArrivalEvent(arrival_process.nextInterval()));
		
		// start processing events
		int sampling_countdown = sampling_interval;
		long jobs_processed = 0;
		while (! event_queue.isEmpty()) {
			if (this.server.queueLength() > FJSimulator.QUEUE_STABILITY_THRESHOLD) {
				System.err.println("ERROR: queue exceeded threshold.  The system is unstable.");
				System.exit(0);
			}
			
			QEvent e = event_queue.removeFirst();
			
			if (e instanceof QJobArrivalEvent) {
				jobs_processed++;
				if (((jobs_processed*100)%num_jobs)==0)
					System.err.println("   ... "+(100*jobs_processed/num_jobs)+"%");
				QJobArrivalEvent et = (QJobArrivalEvent) e;
				FJJob job = new FJJob(num_tasks, service_rate);
				job.arrival_time = et.time;
				if (sampling_countdown==0) {
					server.enqueJob(job, true);
					sampling_countdown = sampling_interval;
				} else {
					server.enqueJob(job, false);
				}
				sampling_countdown--;
				
				// schedule the next job arrival
				if (jobs_processed < num_jobs) {
					double interval = arrival_process.nextInterval();
					if ((interval < 0.0) || (interval>1000)) {
						System.err.println("WARNING: inter-arrival time of "+interval);
					}
					double next_time = et.time + interval;
					this.addEvent(new QJobArrivalEvent(next_time));
				}
			} else if (e instanceof QTaskCompletionEvent) {
				QTaskCompletionEvent et = (QTaskCompletionEvent) e;
				server.taskCompleted(et.task.worker, et.time);
			}
		}
	}
	
	
	/**
	 * add an event to the simulation queue
	 * 
	 * @param e
	 */
	public void addEvent(QEvent e) {
		int queue_len = this.event_queue.size();
		if (event_queue.isEmpty()) {
			event_queue.add(e);
		} else if (e.time > this.event_queue.getLast().time) {
			event_queue.add(e);
		} else {
			int i = 0;
			for (QEvent le : this.event_queue) {
				if (le.time >= e.time) {
					event_queue.add(i, e);
					if (DEBUG) System.out.println("inserting event with time "+e.time+" before event "+i+" with time "+le.time);
					break;
				}
				i++;
			}
		}
		
		// do a sanity check
		if ((this.event_queue.size() - queue_len) != 1) {
			System.err.println("ERROR: adding one thing resulted in wrong change to queue len: "+this.event_queue.size()+"  "+queue_len);
			System.err.println("new event time: "+e.time);
			for (QEvent le : this.event_queue) {
				System.err.println(le.time);
			}
		}
		double last_time = 0.0;
		for (QEvent le : this.event_queue) {
			if (le.time < last_time) {
				System.err.println("ERROR: events in queue out of order!");
				System.exit(1);
			}
			last_time = le.time;
		}
	}
	
	
	/**
	 * compute the means of sojourn, waiting, and service times over (almost) all jobs
	 * 
	 * @return
	 */
	public ArrayList<Double> experimentMeans() {
		double sojourn_sum = 0.0;
		double waiting_sum = 0.0;
		double service_sum = 0.0;
		for (FJJob job : server.sampled_jobs) {
			double job_start_time = job.tasks[0].start_time;
			double job_completion_time = job.tasks[0].completion_time;
			for (FJTask task : job.tasks) {
				job_start_time = Math.min(job_start_time, task.start_time);
				job_completion_time = Math.max(job_completion_time, task.completion_time);
			}
			waiting_sum += job_start_time - job.arrival_time;
			sojourn_sum += job_completion_time - job.arrival_time;
			service_sum += job_completion_time - job_start_time;
		}
		
		double total = server.sampled_jobs.size();
		ArrayList<Double> result = new ArrayList<Double>(3 + 1);
		result.add(sojourn_sum/total);
		result.add(waiting_sum/total);
		result.add(service_sum/total);
		result.add(total * 1.0);
		return result;
	}
	
	
	/**
	 * compute the temporal autocorrelation of the job statistics
	 * 
	 * @param outfile_base
	 * @param max_offset
	 */
	public void jobAutocorrelation(String outfile_base, int max_offset) {
		double[] sojourns = new double[server.sampled_jobs.size()];
		double[] waitings = new double[server.sampled_jobs.size()];
		double[] services = new double[server.sampled_jobs.size()];
		int total = server.sampled_jobs.size();
		int i = 0;
		for (FJJob job : server.sampled_jobs) {
			double job_start_time = job.tasks[0].start_time;
			double job_completion_time = job.tasks[0].completion_time;
			for (FJTask task : job.tasks) {
				job_start_time = Math.min(job_start_time, task.start_time);
				job_completion_time = Math.max(job_completion_time, task.completion_time);
			}
			sojourns[i] = job_completion_time - job.arrival_time;
			waitings[i] = job_start_time - job.arrival_time;
			services[i] = job_completion_time - job_start_time;
			i++;
		}
		
		// first we need the means
		double sojourn_sum = 0.0;
		double waiting_sum = 0.0;
		double service_sum = 0.0;
		for (int j=0; j<sojourns.length; j++) {
			sojourn_sum += sojourns[j];
			waiting_sum += waitings[j];
			service_sum += services[j];
		}
		
		double sojourn_mean = sojourn_sum/total;
		double waiting_mean = waiting_sum/total;
		double service_mean = service_sum/total;
		
		// now compute autocorr
		double[] sojourn_ac = new double[max_offset];
		double[] waiting_ac = new double[max_offset];
		double[] service_ac = new double[max_offset];
		for (int j=0; j<(total-max_offset); j++) {
			for (int x=0; x<max_offset; x++) {
				sojourn_ac[x] += (sojourns[j]-sojourn_mean)*(sojourns[j+x]-sojourn_mean);
				waiting_ac[x] += (waitings[j]-waiting_mean)*(waitings[j+x]-waiting_mean);
				service_ac[x] += (services[j]-service_mean)*(services[j+x]-service_mean);
			}
		}
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_autocor.dat"));
			for (int x=0; x<max_offset; x++) {
				writer.write(x
						+"\t"+sojourn_ac[x]/(total-max_offset)
						+"\t"+waiting_ac[x]/(total-max_offset)
						+"\t"+service_ac[x]/(total-max_offset)
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
	 * Print out the experiment path (if it's small enough) and the CDFs and PDFs
	 * of sojourn, waiting, and service times
	 * 
	 * @param outfile_base
	 */
	public void printExperimentPath(String outfile_base) {
		double binwidth = 0.1;
		
		// max value for the distributions
		double max_value = 0;
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_path.dat"));
			for (FJJob job : server.sampled_jobs) {
				double job_start_time = job.tasks[0].start_time;
				double job_completion_time = job.tasks[0].completion_time;
				for (FJTask task : job.tasks) {
					job_start_time = Math.min(job_start_time, task.start_time);
					job_completion_time = Math.max(job_completion_time, task.completion_time);
				}
				double job_sojourn = job_completion_time - job.arrival_time;
				if (job_sojourn > 10000) {
					System.err.println("WARNING: large job sojourn: "+job_sojourn);
					System.err.println("completion: "+job_completion_time);
					System.err.println("arrival:    "+job.arrival_time);
					System.exit(1);
				}
				max_value = Math.max(max_value, job_sojourn);
				if (server.sampled_jobs.size() < 1000) {
					for (FJTask task : job.tasks) {
						writer.write(task.ID
								+"\t"+job.ID
								+"\t"+job.arrival_time
								+"\t"+job_start_time
								+"\t"+job_completion_time
								+"\t"+task.start_time
								+"\t"+task.completion_time
								+"\n");
					}
				}
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
		
		// initialize the distributions
		int max_bin = (int)(max_value/binwidth) + 1;
		//System.err.println("max_value="+max_value);
		
		int[] job_sojourn_d = new int[max_bin];
		int[] job_waiting_d = new int[max_bin];
		int[] job_service_d = new int[max_bin];
		
		// compute the distributions
		int total = server.sampled_jobs.size();
		for (FJJob job : server.sampled_jobs) {
			
			double job_start_time = job.tasks[0].start_time;
			double job_completion_time = job.tasks[0].completion_time;
			for (FJTask task : job.tasks) {
				job_start_time = Math.min(job_start_time, task.start_time);
				job_completion_time = Math.max(job_completion_time, task.completion_time);
			}
			double job_waiting_time = job_start_time - job.arrival_time;
			double job_sojourn_time = job_completion_time - job.arrival_time;
			double job_service_time = job_completion_time - job_start_time;
			job_sojourn_d[(int)(job_sojourn_time/binwidth)]++;
			job_waiting_d[(int)(job_waiting_time/binwidth)]++;
			job_service_d[(int)(job_service_time/binwidth)]++;
		}
		
		// print out the distributions
		// plot filename using 2:(log($3)) with lines title "sojourn", filename using 2:(log($4)) with lines title "waiting", filename using 2:(log($5)) with lines title "service"
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_dist.dat"));
			double sojourn_cdf = 0.0;
			double waiting_cdf = 0.0;
			double service_cdf = 0.0;
			for (int i=0; i<max_bin; i++) {
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
	  * main()
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args == null || args.length != 8) {
			System.out.println("usage: FJSimulator <queue_type> <num_workers> <num_tasks> <arrival_rate> <service_rate> <numjobs> <sampling_interval> <filename_base>");
			System.exit(0);
		}
		String server_queue_type = args[0];
		int num_workers = Integer.parseInt(args[1]);
		int num_tasks = Integer.parseInt(args[2]);
		double arrival_rate = Double.parseDouble(args[3]);
		double service_rate = Double.parseDouble(args[4]);
		long num_jobs = Long.parseLong(args[5]);
		int sampling_interval = Integer.parseInt(args[6]);
		String outfile_base = args[7];
		
		// set this to be whatever you want
		IntertimeProcess arrival_process = new ExponentialIntertimeProcess(arrival_rate);
		
		FJSimulator sim = new FJSimulator(server_queue_type, num_workers, num_tasks, arrival_process, service_rate);
		sim.run(num_jobs, sampling_interval);
		
		sim.printExperimentPath(outfile_base);
		
		ArrayList<Double> means = sim.experimentMeans();
		System.out.println(
				num_workers
				+"\t"+num_tasks
				+"\t"+arrival_rate
				+"\t"+service_rate
				+"\t"+means.get(0)
				+"\t"+means.get(1)
				+"\t"+means.get(2)
				+"\t"+means.get(3));
		
		//sim.jobAutocorrelation(outfile_base, 5000);
	}

}
