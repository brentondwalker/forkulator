package forkulator;

import java.util.LinkedList;
import java.util.Queue;

public class FJSplitMergeServer extends FJServer {

	/**
	 * This type of server has a single job queue, and jobs wait there
	 * until all tasks from the previous job are fully serviced.
	 * 
	 * When a job comes up for service it's tasks are queued directly
	 * with the workers.
	 */
	public Queue<FJJob> job_queue = new LinkedList<FJJob>();

	
	/**
	 * This type of server has a separate queue for each worker.
	 * 
	 * When a job is enqueued, its tasks are queued on the workers directly,
	 * no mater the current state of the workers.
	 * 
	 */
	private int worker_index = 0;

	
	/**
	 * Constructor
	 * 
	 * Has to allocate the workers' task queues.
	 * 
	 * @param num_workers
	 */
	public FJSplitMergeServer(int num_workers) {
		super(num_workers);
		System.out.println("FJSplitMergeServer()");
		
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
		boolean all_idle = true;
		
		// check for idle workers
		for (int i=0; i<num_workers; i++) {
			if (workers[0][i].current_task == null) {
				// if the worker is idle, pull the next task (or null) from its queue
				serviceTask(workers[0][i], workers[0][i].queue.poll(), time);
			}
			
			// check if a task is being serviced now
			if (workers[0][i].current_task != null) {
				all_idle = false;
			}
		}
		
		// if all the workers are idle it means we have completed the current
		// batch of tasks.  Grab the next job and enqueue it
		if (all_idle) {
			ServiceJob(this.job_queue.poll(), time);
		}
	}
	
	
	/**
	 * Enqueue a new job.
	 * 
	 * This type of server has a separate queue for each worker.  When a job is ready
	 * for service we assign its tasks to the workers' queues.
	 * 
	 * @param job
	 */
	public void ServiceJob(FJJob job, double time) {
		if (job == null) return;
		if (FJSimulator.DEBUG) System.out.println("begin service on job: "+job.path_log_id+"    "+time);
		
		FJTask t = null;
		while ((t = job.nextTask()) != null) {
			workers[0][worker_index].queue.add(t);
			worker_index = (worker_index + 1) % num_workers;
		}
		
		// this just added the tasks to the queues.  Check if any
		// workers were idle, and put them to work.
		feedWorkers(time);
	}

	
	/**
	 * Enqueue a new job.
	 * 
	 * This server type has a single job queue where jobs wait until the previous job
	 * completes and departs.
	 * 
	 * @param job
	 * @param sample
	 */
	public void enqueJob(FJJob job, boolean sample) {
		if (FJSimulator.DEBUG) System.out.println("enqueJob("+job.path_log_id+") "+job.arrival_time);
		
		// only keep a reference to the job if the simulator tells us to
		job.setSample(sample);
		
		job_queue.add(job);
		feedWorkers(job.arrival_time);
		if (FJSimulator.DEBUG) System.out.println("  queued a job.    New queue length: "+job_queue.size());
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
		worker.current_task = null;
		
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
			if (FJSimulator.DEBUG) System.out.println("job departing: "+task.job.path_log_id);
			jobDepart(task.job);
			
			// service the next job, if any
			feedWorkers(time);
		} else {
			serviceTask(worker, worker.queue.poll(), time);
		}
	}


	/**
	 * In the multi-queue server we take the queue length to be the rounded average
	 * length of all the worker queues.
	 */
	@Override
	public int queueLength() {
	    return this.job_queue.size();
	}

	@Override
	int numJobsInQueue() {
		return job_queue.size();
	}
	
	
	
}
