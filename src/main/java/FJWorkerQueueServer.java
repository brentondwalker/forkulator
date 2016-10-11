package forkulator;

import java.util.LinkedList;

public class FJWorkerQueueServer extends FJServer {

	/**
	 * This type of server has a separate queue for each worker.
	 * 
	 * When a job is enqueued, its tasks are queued on the workers directly,
	 * no mater the current state of the workers.
	 */
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
			if (workers[0][i].current_task == null) {
				// if the worker is idle, pull the next task (or null) from its queue
				serviceTask(workers[0][i], workers[0][i].queue.poll(), time);
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
		if (sample) {
			job.setSample(sample);
			//sampled_jobs.add(job);
		}

		FJTask t = null;
		while ((t = job.nextTask()) != null) {
			workers[0][worker_index].queue.add(t);
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
	public void taskCompleted(FJWorker worker, double time) {
		if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.ID+" completed "+time);
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
			
			// dispose of the job
			task.job.dispose();
		}
		
		serviceTask(worker, worker.queue.poll(), time);
		
	}


	/**
	 * In the multi-queue server we take the queue length to be the rounded average
	 * length of all the worker queues.
	 */
	@Override
	public int queueLength() {
		int lsum = 0;
		for (FJWorker w : workers[0])
			lsum += w.queue.size();

		return (lsum / num_workers);
	}
	
	
}
