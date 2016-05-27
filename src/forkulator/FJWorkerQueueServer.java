package forkulator;

import java.util.LinkedList;
import java.util.Queue;

public class FJWorkerQueueServer extends FJServer {

	/**
	 * This type of server has a separate queue for each worker.
	 * 
	 * When a job is enqueued, its tasks are queued on the workers directly,
	 * no mater the current state of the workers.
	 */
	public Queue<FJTask>[] worker_queues = null;
	private int worker_index = 0;
	
	/**
	 * Constructor
	 * 
	 * Has to allocate the workers' task queues.
	 * 
	 * @param num_workers
	 */
	public FJWorkerQueueServer(int num_workers) {
		super(num_workers);
		
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
	 * @param job
	 * @param sample
	 */
	public void enqueJob(FJJob job, boolean sample) {
		if (FJSimulator.DEBUG) System.out.println("enqueJob() "+job.arrival_time);

		// only keep a reference to the job if the simulator tells us to
		if (sample)
			sampled_jobs.add(job);

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
	 * @param workerId
	 * @param time
	 */
	public void taskCompleted(int workerId, double time) {
		if (FJSimulator.DEBUG) System.out.println("task "+workers[workerId].current_task.ID+" completed "+time);
		workers[workerId].current_task.completion_time = time;
		workers[workerId].current_task.completed = true;
		
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
