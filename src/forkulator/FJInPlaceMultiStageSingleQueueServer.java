package forkulator;

import java.util.LinkedList;
import java.util.Queue;

public class FJInPlaceMultiStageSingleQueueServer extends FJServer {

	/**
	 * The in-place multi-stage system has a single set of workers
	 * that service jobs with multiple stages  In the default case the
	 * tasks must re-synchronize between stages.  This is simulating
	 * the situation in Spark where there is a shuffle/reduce between the
	 * map stages.  There is also the option to pipeline the tasks, so that
	 * each task becomes the concatenation of the r stages.  This is 
	 * simulating the situation in Spark where there are a series of map
	 * operations without any shuffle/reduce in between.
	 * 
	 * There's a lot of fancy customization possible here, but I
	 * will start by making it simple:
	 * - systems are all in series
	 * - jobs must synchronize between stages (like spark, unlike some others)
	 * - all systems have the same parallelism and tasks/job
	 * - all systems have same service parameters
	 * 
	 */
		
	/**
	 * The worker index keeps track of which worker the last task was assigned to.
	 * In the multi-stage system we need a worker_index for each stage.
	 * 
	 * In the single queue system we will not be using this....
	 */
	private int worker_index[] = null;
	
	/**
	 * Optionally re-sample the service times of the tasks between stages.
	 * If this is false the service times of the tasks will be 100% correlated
	 * across stages, which changes the behavior of the system.
	 */
	public boolean independent_stages = false;
	
	/**
	 * This type of server has a single job queue, and tasks are drawn from
	 * the job at the front of the queue until the job is fully serviced.
	 * 
	 * The way I implemented this the jobs in job_queue are not serviced at all
	 * yet.  However there can be several jobs in service at any time, so
	 * I keep those in the active_jobs list.
	 */
	public Queue<FJJob> job_queue = new LinkedList<FJJob>();
	public LinkedList<FJJob> active_jobs = new LinkedList<FJJob>();
	
	/**
	 * Constructor
	 * 
	 * Has to allocate the workers' task queues.
	 * 
	 * @param num_workers
	 */
	public FJInPlaceMultiStageSingleQueueServer(int num_workers, int num_stages, boolean independent_stages) {
		super(num_workers);
		
		this.num_stages = num_stages;
		this.independent_stages = independent_stages;
		
		this.worker_index = new int[num_stages];
		for (int j=0; j<num_stages; j++)
			worker_index[j] = 0;
		
		this.workers = new FJWorker[1][num_workers];
		for (int i=0; i<num_workers; i++) {
			workers[1][i] = new FJWorker(0);
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
	 * Check for any idle workers and try to put a task on them.
	 * 
	 * In the in-place multi-stage system this is complicated because we can't always
	 * take the next task from the current job.  If the jobs are re-synchronizing
	 * between stages, then the head job may be idling while it waits for tasks to
	 * complete their current stage.  In that case we go deeper into the list of
	 * jobs to find one that has tasks that are ready to run.
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
		job.setSample(sample);
		
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
		
		if (compl) {
			if ((worker.stage + 1) == num_stages) {
				// this job is complete at the last stage
				// it is the last, record the completion time
				task.job.completion_time = time;
				
				// for this type of server it is also the departure time
				task.job.departure_time = time;
				
				// sample and dispose of the job
				jobDepart(task.job);
				
			} else {
				// this job has completed an intermediate stage.
				//
				// reset the tasks to completed=false and queue them
				// with the next stage
				// optionally resample the tasks' service times
				int s = worker.stage + 1;
				for (FJTask t : task.job.tasks) {
					t.completed = false;
					if (independent_stages) {
						t.resampleServiceTime(time);
					}
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
