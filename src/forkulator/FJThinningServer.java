package forkulator;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.TreeSet;

public class FJThinningServer extends FJServer {
	
	/**
	 * The thinning server is special because
	 * - jobs are mapped entirely to one server
	 * - the jobs (optionally) must be resequenced before they depart
	 * 
	 * The 2nd fact requires us to keep a queue pile of jobs that have completed
	 * servicing, but are still waiting to depart. 
	 */
	public TreeSet<FJJob> postservice_jobs = new TreeSet<FJJob>();
	protected long job_departure_index = -1;
	
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
	 * The assignment of jobs to servers can be random or deterministic.
	 */
	boolean random_thinning = false;
	protected static Random rand = new Random();
	
	/**
	 * We optionally can require resequencing of jobs before they depart.
	 * With reqsequencing job n cannot depart before job n-1
	 */
	boolean resequencing = false;
	
	
	/**
	 * Allocate a FJJob queue for each worker.
	 * 
	 * @param num_workers
	 * @param random_thinning
	 * @param resequencing
	 */
	public FJThinningServer(int num_workers, boolean random_thinning, boolean resequencing) {
		super(num_workers);
		
		this.random_thinning = random_thinning;
		
		this.resequencing = resequencing;
		
		for (int i=0; i<num_workers; i++) {
			workers[0][i].queue = new LinkedList<FJTask>();
		}
	}
	
	public FJThinningServer(int num_workers, boolean random_thinning) {
		this(num_workers, random_thinning, false);
	}
	
	public FJThinningServer(int num_workers) {
		this(num_workers, false);
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
	 * In a thinning server, all tasks from a job go on the same server.
	 * The server can be chosen randomly or deterministically (round-robin).
	 * 
	 * Note: with the thinning server you could have k tasks with exponential service times
	 *       per job, or equivalently one task with erlang-k service time, which should
	 *       run faster.
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
		int wi = -1;
		if (random_thinning) {
			wi = rand.nextInt(this.num_workers);
		} else {
			wi = worker_index;
			worker_index = (worker_index + 1) % num_workers;
		}
		while ((t = job.nextTask()) != null) {
			workers[0][wi].queue.add(t);
		}
		
		// this just added the tasks to the queues.  Check if any
		// workers were idle, and put them to work.
		feedWorkers(job.arrival_time);
		
		// if we are doing resequencing, add it to the departure set
		// with resequencing on, no job can depart before all te jobs older than it have departed
		if (resequencing) {
			postservice_jobs.add(job);
		}
	}
	
	
	/**
	 * Handle a task completion event.  Remove the task from its worker, and
	 * give the worker a new task, if available.
	 * 
	 * With in the thinning server the task can't simply depart, though.  The
	 * jobs need to be resequenced.  If a job has completed, we put it into the
	 * departure queue and then try to clear any jobs from the departure queue.
	 * 
	 * @param workerId
	 * @param time
	 */
	
	public void taskCompleted(FJWorker worker, double time) {
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
			
			// if we are resequencing, the job can only depart if it is the oldest job still in the system
			if (resequencing) {
				// check if any postservice_jobs can be cleared
				while ((! postservice_jobs.isEmpty()) && (postservice_jobs.first() == task.job)) {
					FJJob j = postservice_jobs.pollFirst();
					job_departure_index++;
					j.departure_time = time;
					
					// sample and dispose of the job
					jobDepart(j);
				}
			} else {
				// otherwise the job departs immediately
				task.job.departure_time = time;

				// sample and dispose of the job
				jobDepart(task.job);
			}
		}
		
		// try to start servicing a new task on this worker
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
