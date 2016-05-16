package forkulator;

import java.util.LinkedList;
import java.util.Queue;

public class FJSingleQueueServer extends FJServer {

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
	 * Since we allocate the single job_queue above, there is nothing
	 * special for this constructor to do except call super().
	 * 
	 * @param num_workers
	 */
	public FJSingleQueueServer(int num_workers) {
		super(num_workers);
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
	
	/**
	 * Enqueue a new job.
	 * 
	 * This server type has a single job queue, and tasks are pulled from the job
	 * at the head of the queue until they're exhausted.
	 * 
	 * @param job
	 * @param sample
	 */
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
	
	
	/**
	 * Handle a task completion event.  Remove the task from its worker, and
	 * give the worker a new task, if available.
	 * 
	 * @param workerId
	 * @param time
	 */
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
