package forkulator;

import java.util.LinkedList;

public class FJMultiStageWorkerQueueServer extends FJServer {

	/**
	 * The multi-stage system has several FJ systems in series.
	 * There's a lot of fancy customization possible here, but I
	 * will start by making it simple:
	 * - systems are all in series
	 * - jobs must synchronize between stages (like spark, unlike some others)
	 * - all systems have the same parallelism and tasks/job
	 * - all systems have same service parameters
	 * 
	 * It is tempting to build this system out of several FJWorkerQueueServer,
	 * but that would be a difficult because the actions the server takes on
	 * task completion include cleaning up the task and job and reporting statistics.
	 * To do it that way would involve a lot of refactoring.
	 */
		
	/**
	 * The worker index keeps track of which worker the last task was assigned to.
	 * In the multi-stage system we need a worker_index for each stage.
	 */
	private int worker_index[] = null;

	/**
	 * The superclass has an array of workers, but we need more workers for the
	 * multi-stage system.
	 */
	public FJWorker[][] workers = null;
	
	
	/**
	 * Constructor
	 * 
	 * Has to allocate the workers' task queues.
	 * 
	 * @param num_workers
	 */
	public FJMultiStageWorkerQueueServer(int num_workers, int num_stages) {
		super(num_workers);
		
		this.num_stages = num_stages;
		
		this.worker_index = new int[num_stages];
		for (int j=0; j<num_stages; j++)
			worker_index[j] = 0;
		
		this.workers = new FJWorker[num_stages][num_workers];
		for (int j=0; j<num_stages; j++) {
			for (int i=0; i<num_workers; i++) {
				workers[j][i] = new FJWorker(j);
				workers[j][i].queue = new LinkedList<FJTask>();
			}
		}
	}
	
	
	/**
	 * Check for any idle workers and try to put a task on them.
	 * 
	 * @param time
	 * @param stage
	 */
	public void feedWorkers(double time, int stage) {
		for (int i=0; i<num_workers; i++) {
			if (workers[stage][i].current_task == null) {
				// if the worker is idle, pull the next task (or null) from its queue
				serviceTask(workers[stage][i], workers[stage][i].queue.poll(), time);
			}
		}
	}
	
	/**
	 * Feed idle workers in all stages
	 * 
	 * @param time
	 */
	public void feedWorkers(double time) {
		// check for idle workers
		for (int j=0; j<num_stages; j++) {
			feedWorkers(time, j);
		}
	}
	
	
	/**
	 * Enqueue a new job.
	 * 
	 * This type of server has a separate queue for each worker.  When a job arrives
	 * we immediately assign its tasks to the workers' queues.
	 * 
	 * This method is called externally, and in the multi-stage system it means
	 * we enqueue a job on the first stage of the system.
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
			workers[0][worker_index[0]].queue.add(t);
			worker_index[0] = (worker_index[0] + 1) % num_workers;
		}
		
		// this just added the tasks to the queues.  Check if any
		// workers (in stage 0) were idle, and put them to work.
		feedWorkers(job.arrival_time, 0);
	}
	
	
	/**
	 * Handle a task completion event.  Remove the task from its worker, and
	 * give the worker a new task, if available.
	 * 
	 * Check if the task is the last of a job, and if so pass the job
	 * to the next stage.  If it is the last stage, do the job completion
	 * processing.
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
		
		if (compl) {
			if ((worker.stage + 1) == num_stages) {
				// this job is complete at the last stage
				// it is the last, record the completion time
				task.job.completion_time = time;
				
				// for this type of server it is also the departure time
				task.job.departure_time = time;
				
				// dispose of the job
				task.job.dispose();
			} else {
				// this job has completed an intermediate stage.
				//
				// reset the tasks to completed=false and queue them
				// with the next stage
				int s = worker.stage + 1;
				for (FJTask t : task.job.tasks) {
					t.completed = false;
					workers[s][worker_index[s]].queue.add(t);
					worker_index[s] = (worker_index[s] + 1) % num_workers;
				}
				this.feedWorkers(time, s);
			}
		}
		
		// start servicing another task on the worker that just finished this task
		serviceTask(worker, worker.queue.poll(), time);
	}


	/**
	 * In the multi-queue server we take the queue length to be the rounded average
	 * length of all the worker queues.
	 * 
	 * In the multi-stage/multi-queue system we take the rounded average of the
	 * length of the sum across all stages.
	 */
	@Override
	public int queueLength() {
		int lsum = 0;
		for (int j=0; j<num_stages; j++) {
			for (int i=0; i<num_workers; i++) {
				lsum += workers[j][i].queue.size();
			}
		}
		return (lsum / num_workers);
	}
	
	
}
