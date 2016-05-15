package forkulator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class FJServer {

	private static FJSimulator simulator = null;
	
	public int num_workers = 0;
	public FJWorker[] workers = null;
	public Queue<FJJob> job_queue = new LinkedList<FJJob>();
	public FJJob current_job = null;
	public ArrayList<FJJob> sampled_jobs = new ArrayList<FJJob>();
	
	public FJServer(int num_workers) {
		this.num_workers = num_workers;
		this.workers = new FJWorker[num_workers];
		for (int i=0; i<num_workers; i++) {
			workers[i] = new FJWorker();
		}
	}
	
	public static void setSimulator(FJSimulator simulator) {
		FJServer.simulator = simulator;
	}
	
	public void feedWorkers(double time) {
		// check for idle workers
		for (int i=0; i<num_workers; i++) {

			// if there is no current job, just return
			if (current_job == null) return;
			
			if (workers[i].current_task == null) {
				// service the next task
				serviceTask(i, current_job.nextTask(), time);
				
				// if the current job is exhausted, grab a new one (or null)
				if (current_job.complete) {
					current_job = job_queue.poll();
				}
			}
		}
	}
	
	public void enqueJob(FJJob job, boolean sample) {
		if (FJSimulator.DEBUG) System.out.println("enqueJob() "+job.arrival_time);

		// only keep a reference to the job if the simulator tells us to
		if (sample)
			sampled_jobs.add(job);
		if (current_job == null) {
			current_job = job;
			if (FJSimulator.DEBUG) System.out.println("  current job was null");
			feedWorkers(job.arrival_time);
		} else {
			job_queue.add(job);
			if (FJSimulator.DEBUG) System.out.println("  current job was full, so I queued this one");
		}
	}
	
	public void serviceTask(int workerId, FJTask task, double time) {
		if (FJSimulator.DEBUG) System.out.println("serviceTask() "+task.ID);
		workers[workerId].current_task = task;
		task.worker = workerId;
		task.start_time = time;
		task.processing = true;
		
		// schedule the task's completion
		QTaskCompletionEvent e = new QTaskCompletionEvent(task, time + task.service_time);
		simulator.addEvent(e);
	}
	
	public void taskCompleted(int workerId, double time) {
		if (FJSimulator.DEBUG) System.out.println("task "+workers[workerId].current_task.ID+" completed "+time);
		workers[workerId].current_task.completion_time = time;
		workers[workerId].current_task.completed = true;
		
		// if there is no current job, just clear the worker
		if (current_job == null) {
			if (FJSimulator.DEBUG) System.out.println("  no current_job");
			workers[workerId].current_task = null;
			return;
		}
		
		// put a new task on the worker
		serviceTask(workerId, current_job.nextTask(), time);
		
		// if the current job is exhausted, grab a new one (or null)
		if (current_job.complete) {
			current_job = job_queue.poll();
			if (FJSimulator.DEBUG) System.out.println("  set current_job to "+current_job);
			feedWorkers(time);  // this should not do anything
		}
	}
	
	
}
