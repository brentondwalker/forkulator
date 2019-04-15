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
	 * This server type has a single job queue, and tasks are pulled from the job
	 * at the head of the queue until they're exhausted.
	 * 
	 * @param job
	 * @param sample
	 */
	public void enqueJob(FJJob job, boolean sample) {
		if (FJSimulator.DEBUG) System.out.println("enqueJob() "+job.arrival_time);

		// only keep a reference to the job if the simulator tells us to
		job.setSample(sample);
		
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
	public void taskCompleted(FJWorker worker, double time) {
		//if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.ID+" completed "+time);
		FJTask task = worker.current_task;
		task.completion_time = time;
		task.completed = true;

		// check if this task was the last one of a job
		//TODO: this could be more efficient
		boolean compl = true;
		for (FJTask t : task.job.tasks) {
			compl = compl && t.completed;
		}
		task.job.completed = compl;

		if (task.job.completed) {
			// it is the last, record the completion time
			task.job.completion_time = time;
			
			// for this type of server it is also the departure time
			task.job.departure_time = time;
			
			// sample and dispose of the job
			jobDepart(task.job);
			current_job = null;
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
