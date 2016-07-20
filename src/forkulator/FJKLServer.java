package forkulator;

import java.util.LinkedList;
import java.util.Queue;


public class FJKLServer extends FJServer {
	
	/**
	 * The (k,l) server considers a job done when l of its k tasks complete.
	 * The remaining tasks are still left to run to completion.
	 * 
	 */
	public int l;
	
	/**
	 * This type of server has a separate queue for each worker.
	 * 
	 * When a job comes in all of its tasks are put on the same worker.
	 * We could model this with a Queue<FJJob>, but it's easier and just 
	 * as valid to copy the model used by the FJWorkerQueueServer
	 */
	public Queue<FJTask>[] worker_queues = null;
	private int worker_index = 0;
	
	
	/**
	 * Constructor
	 * 
	 * Allocate a FJJob queue for each worker.
	 * 
	 * @param num_workers
	 */
	public FJKLServer(int num_workers, int l) {
		super(num_workers);
		
		if (l > num_workers) {
			System.err.println("ERROR: FJKLServer cannot have l>k");
			System.exit(0);
		}
		
		this.l = l;
		worker_queues = new Queue[num_workers];
		for (int i=0; i<num_workers; i++) {
			worker_queues[i] = new LinkedList<FJTask>();
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
			if (workers[i].current_task == null) {
				// if the worker is idle, pull the next task (or null) from its queue
				serviceTask(i, worker_queues[i].poll(), time);
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
			sampled_jobs.add(job);
		}

		FJTask t = null;
		while ((t = job.nextTask()) != null) {
			worker_queues[worker_index].add(t);
			worker_index = (worker_index + 1) % num_workers;
		}
		
		// this just added the tasks to the queues.  Check if any
		// workers were idle, and put them to work.
		feedWorkers(job.arrival_time);
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
	public void taskCompleted(int workerId, double time) {
		if (FJSimulator.DEBUG) System.out.println("task "+workers[workerId].current_task.ID+" completed "+time);
		FJTask task = workers[workerId].current_task;
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
				FJJob j = task.job;
				j.dispose();
			}
		}
		
		// try to start servicing a new task on this worker
		serviceTask(workerId, worker_queues[workerId].poll(), time);
	}
	
	
	/**
	 * In the multi-queue server we take the queue length to be the rounded average
	 * length of all the worker queues.
	 */
	@Override
	public int queueLength() {
		int lsum = 0;
		for (Queue<FJTask> q : worker_queues) {
			lsum += q.size();
		}
		return (lsum / worker_queues.length);
	}
	
	
}
