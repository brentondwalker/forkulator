package forkulator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.Random;

public class FJSimulator {

	public static boolean DEBUG = false;
	private static Random rand = new Random();
	
	public LinkedList<QEvent> event_queue = new LinkedList<QEvent>();
	
	public int num_workers;
	public int num_tasks;
	public double arrival_rate;
	public double service_rate;
	public FJServer server = null;
	
	public FJSimulator(int num_workers, int num_tasks, double arrival_rate, double service_rate) {
		this.num_workers = num_workers;
		this.num_tasks = num_tasks;
		this.arrival_rate = arrival_rate;
		this.service_rate = service_rate;
		
		this.server = new FJServer(num_workers);
		
		FJServer.setSimulator(this);
	}
	
	public void run(int num_jobs) {
		// we can generate all the job arrivals at once
		double current_time = 0.0;
		//System.out.println("Arrival times:");
		for (int i=0; i<num_jobs; i++) {
			double next_time = current_time + -Math.log(rand.nextDouble())/arrival_rate;
			event_queue.add(new QJobArrivalEvent(next_time));
			current_time = next_time;
			//System.out.println("\t"+next_time);
		}
		
		// start processing events
		while (! event_queue.isEmpty()) {
			QEvent e = event_queue.removeFirst();
			
			if (e instanceof QJobArrivalEvent) {
				QJobArrivalEvent et = (QJobArrivalEvent) e;
				FJJob job = new FJJob(num_tasks, service_rate);
				job.arrival_time = et.time;
				server.enqueJob(job);
			} else if (e instanceof QTaskCompletionEvent) {
				QTaskCompletionEvent et = (QTaskCompletionEvent) e;
				server.taskCompleted(et.task.worker, et.time);
			}
		}
		
	}
	
	public void addEvent(QEvent e) {
		if (event_queue.isEmpty()) {
			event_queue.add(e);
		} else if (e.time > this.event_queue.getLast().time) {
			event_queue.add(e);
		} else {
			int i = 0;
			for (QEvent le : this.event_queue) {
				if (le.time > e.time) {
					event_queue.add(i, e);
					if (DEBUG) System.out.println("inserting event with time "+e.time+" before event "+i+" with time "+le.time);
					break;
				}
				i++;
			}
		}
	}
	
	public void printExperimentPath(String outfile_base) {
		double binwidth = 0.1;
		
		// max value for the distribuions
		double max_value = 0;
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_path.dat"));
			for (FJJob job : server.all_jobs) {
				double job_start_time = job.tasks[0].start_time;
				double job_completion_time = job.tasks[0].completion_time;
				for (FJTask task : job.tasks) {
					job_start_time = Math.min(job_start_time, task.start_time);
					job_completion_time = Math.max(job_completion_time, task.completion_time);
				}
				double job_sojourn = job_completion_time - job.arrival_time;
				max_value = Math.max(max_value, job_sojourn);
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
		
		int[] job_sojourn_d = new int[max_bin];
		int[] job_waiting_d = new int[max_bin];
		int[] job_service_d = new int[max_bin];
		
		// compute the distributions
		for (FJJob job : server.all_jobs) {
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
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_dist.dat"));
			double total = 1.0 * server.all_jobs.size();
			for (int i=0; i<max_bin; i++) {
				writer.write(i
						+"\t"+(i*binwidth)
						+"\t"+(1.0*job_sojourn_d[i])/total
						+"\t"+(1.0*job_waiting_d[i])/total
						+"\t"+(1.0*job_service_d[i])/total
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
		int num_workers = Integer.parseInt(args[0]);
		int num_tasks = Integer.parseInt(args[1]);
		double arrival_rate = Double.parseDouble(args[2]);
		double service_rate = Double.parseDouble(args[3]);
		int num_jobs = Integer.parseInt(args[4]);
		String outfile_base = args[5];
		
		FJSimulator sim = new FJSimulator(num_workers, num_tasks, arrival_rate, service_rate);
		sim.run(num_jobs);
		
		sim.printExperimentPath(outfile_base);
	}

}
