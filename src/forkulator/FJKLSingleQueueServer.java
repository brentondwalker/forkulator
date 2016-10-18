package forkulator;

import java.util.LinkedList;
import java.util.Queue;


public class FJKLSingleQueueServer extends FJServer {
	
	/**
	 * The (k,l) server considers a job done when l of its k tasks complete.
	 * The remaining tasks are still left to run to completion.
	 * 
	 */
	public int l;

	
	/**
	 * This type of server has a single job queue, and tasks are drawn from
	 * the job at the front of the queue until the job is fully serviced.
	 * 
	 * The way I implemented this, I put the current "head" job in current_job,
	 * and the jobs in the queue are the ones not serviced yet.
	 */
	public Queue<FJJob> job_queue = new LinkedList<FJJob>();
	public FJJob current_job = null;
		
	
	/**
	 * Constructor
	 * 
	 * Allocate a FJJob queue for each worker.
	 * 
	 * @param num_workers
	 */
	public FJKLSingleQueueServer(int num_workers, int l) {
		super(num_workers);
		
		if (l > num_workers) {
			System.err.println("ERROR: FJKLSingleQueueServer cannot have l>k");
			System.exit(0);
		}
		
		this.l = l;
		for (int i=0; i<num_workers; i++) {
			workers[0][i].queue = new LinkedList<FJTask>();
		}
	}
	
	
	/**
	 * Check for any idle workers and try to put a task on them.
	 * 
	 * @param time
	 */
	public void feedWorkers(double time) {
		// check for idle workers
		for (int i=0; i<num_workers; i++) {

			// if there is no current job, just return
			if (current_job == null) return;
			
			if (workers[0][i].current_task == null) {
				// service the next task
				serviceTask(workers[0][i], current_job.nextTask(), time);
				
				// if the current job is exhausted, grab a new one (or null)
				if (current_job.fully_serviced) {
					current_job = job_queue.poll();
				}
			}
		}
	}
	
	
	/**
	 * Enqueue a new job.
	 * 
	 * This type of server has a separate queue for each worker.  When a job arrives
	 * we immediately assign its tasks to the workers' queues.
	 * 
	 * This part should behave just like WQ server.
	 * 
	 * @param job
	 * @param sample
	 */
	public void enqueJob(FJJob job, boolean sample) {
		if (FJSimulator.DEBUG) System.out.println("enqueJob() "+job.arrival_time);

		// only keep a reference to the job if the simulator tells us to
		if (sample) {
			job.setSample(sample);
			//sampled_jobs.add(job);
		}
		if (current_job == null) {
			current_job = job;
			if (FJSimulator.DEBUG) System.out.println("  current job was null");
			feedWorkers(job.arrival_time);
		} else {
			job_queue.add(job);
			if (FJSimulator.DEBUG) System.out.println("  current job was full, so I queued this one");
		}
	}
	
	
	/**
	 * Handle a task completion event.  Remove the task from its worker, and
	 * give the worker a new task, if available.
	 * 
	 * With in the (k,l) server we consider a job to be complete and
	 * departed when k of its tasks are complete.  But we let the other
	 * tasks keep running.  Do we need to keep the completed jobs around until
	 * all their tasks finish (if they aren't sampled)?
	 * 
	 * @param workerId
	 * @param time
	 */
	public void taskCompleted(FJWorker worker, double time) {
		//if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.ID+" completed "+time);
		FJTask task = worker.current_task;
		task.completion_time = time;
		task.completed = true;
		
		if (! task.job.completed) {
			// check if this task is the l'th of the job.
			//TODO: this could be more efficient
			int num_completed = 0;
			for (FJTask t : task.job.tasks) {
				num_completed += t.completed ? 1 : 0;
			}
			
			if (num_completed == this.l) {
				task.job.completed = true;
				task.job.completion_time = time;
				task.job.departure_time = time;
				//System.out.println("Job "+task.job.ID+" completed at "+time);
			}
		}
		
		// if the job is already complete, check if this was the last task
		// so we can dispose the job object
		if (task.job.completed) {
			boolean total_complete = true;
			for (FJTask t : task.job.tasks) {
				total_complete = total_complete && t.completed;
			}
			if (total_complete) {
				// sample and dispose of the job
				jobDepart(task.job);
			}
		}
		
		// if there is no current job, just clear the worker
		if (current_job == null) {
			if (FJSimulator.DEBUG) System.out.println("  no current_job");
			worker.current_task = null;
			return;
		}
		
		// put a new task on the worker
		serviceTask(worker, current_job.nextTask(), time);
		
		// if the current job is exhausted, grab a new one (or null)
		if (current_job.fully_serviced) {
			current_job = job_queue.poll();
			if (FJSimulator.DEBUG) System.out.println("  set current_job to "+current_job);
			feedWorkers(time);  // this should not do anything
		}
	}
	
	
	/**
	 * In the single-queue server the queue length is simply the length of the job queue.
	 */
	@Override
	public int queueLength() {
		return this.job_queue.size();
	}
	
}
