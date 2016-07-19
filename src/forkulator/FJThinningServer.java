package forkulator;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

public class FJThinningServer extends FJServer {
	
	/**
	 * The thinning server is special because
	 * - jobs are mapped entirely to one server
	 * - the jobs must be resequenced before they depart
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
	 * Constructor
	 * 
	 * Allocate a FJJob queue for each worker.
	 * 
	 * @param num_workers
	 */
	public FJThinningServer(int num_workers, boolean random_thinning) {
		super(num_workers);
		
		this.random_thinning = random_thinning;
		
		worker_queues = new Queue[num_workers];
		for (int i=0; i<num_workers; i++) {
			worker_queues[i] = new LinkedList<FJTask>();
		}
	}

	public FJThinningServer(int num_workers) {
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
	 * In a thinning server, all tasks from a job go on the same server.
	 * The server can be chosen randomly or deterministically (round-robin).
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
		int wi = -1;
		if (random_thinning) {
			wi = rand.nextInt(this.num_workers);
		} else {
			wi = worker_index;
			worker_index = (worker_index + 1) % num_workers;
		}
		while ((t = job.nextTask()) != null) {
			worker_queues[wi].add(t);
		}
		
		// this just added the tasks to the queues.  Check if any
		// workers were idle, and put them to work.
		feedWorkers(job.arrival_time);
	}
	
	
	/**
	 * Handle a task completion event.  Remove the task from its worker, and
	 * give the worker a new task, if available.
	 * 
	 * With in the thinning server the task can't simply depart, though.  The
	 * jobs need to be resequenced.  If a job has completed, we put it into the
	 * departure queue and then try to clear any jobs from te departure queue.
	 * 
	 * @param workerId
	 * @param time
	 */
	public void taskCompleted(int workerId, double time) {
		if (FJSimulator.DEBUG) System.out.println("task "+workers[workerId].current_task.ID+" completed "+time);
		FJTask task = workers[workerId].current_task;
		task.completion_time = time;
		task.completed = true;
		
		// check if this task was the last one of a job
		//TODO: this could be more efficient
		boolean compl = true;
		for (FJTask t : task.job.tasks) {
			compl = compl && t.completed;
		}
		task.job.complete = compl;
		
		if (task.job.complete) {
			// it is the last, record the completion time
			task.job.completion_time = time;
			
			// and add it to the departure set
			postservice_jobs.add(task.job);
			
			//System.out.println("postservice_jobs: "+postservice_jobs.size());
			//System.out.println("first.ID: "+postservice_jobs.first().ID+"    job_departure_index: "+job_departure_index);
			// check if any postservice_jobs can be cleared
			while ((! postservice_jobs.isEmpty()) && (postservice_jobs.first().ID == (job_departure_index+1))) {
				FJJob j = postservice_jobs.pollFirst();
				job_departure_index++;
				j.departure_time = time;
				//System.out.println("JobID: "+task.job.ID+"  set departure time: "+time);
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
