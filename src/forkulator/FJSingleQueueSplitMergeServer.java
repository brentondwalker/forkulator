package forkulator;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;


public class FJSingleQueueSplitMergeServer extends FJServer {
	/**
	 * This type of server has a single job queue, and tasks are drawn from
	 * the job at the front of the queue until the job is fully serviced.
	 *
	 * The way I implemented this, I put the current "head" job in current_job,
	 * and the jobs in the queue are the ones not serviced yet.
	 * 
	 * The point of this server is to experiment with SplitMerge where
	 * the number of tasks per job  is more than the number of servers,
	 * for example Tiny Tasks.  In FJSplitMergeServer, it assumes that 
	 * the number of tasks per job is less than or equal to the number
	 * of servers.
	 * 
	 */
	public Queue<FJJob> job_queue = new LinkedList<FJJob>();
	public FJJob current_job = null;


	/**
	 * Constructor
	 *
	 * Allocate a FJJob queue for each worker.
	 *
	 * @param num_workers
	 */
	public FJSingleQueueSplitMergeServer(int num_workers) {
		super(num_workers);
	}
	
	
	/**
	 * Check for any idle workers and try to put a task on them.
	 * 
	 * @param time
	 */
	public void feedWorkers(double time) {

		// if there is no current job, just return
		if (current_job == null) return;
		for (int i=0; i<num_workers; i++) {
			// In this server type feedWorkers is only called if a new job is scheduled. So the
			// worker idle time gets reset here. If all servers calculates at lead one task
			// setting the time to current time is not necessary.
			if (workers[0][i].current_task == null) {
				workers[0][i].lastTimeCompleted = time;
				FJTask task = current_job.nextTask();
//				if (overhead_process != null)
//					task.service_time += overhead_process.nextInterval();
				serviceTask(workers[0][i], task, time);
			}
		}
	}

	/**
	 * Calculates the worker idle time by using the given jobFinishTime. Could be merged with
	 * feedWorkers to prevent iterating through the workers multiple time but breaks clear
	 * distinction.
	 * @param job
	 */
	public double getWorkersWaitingTime(FJJob job) {
//		double totalIdleTime = 0.0;
//		for (int i=job.tasks.length-1; job.tasks.length - i + 1 < num_workers; i--) {
//			totalIdleTime += job.completion_time - job.tasks[i].completion_time;
//		}
		return job.completion_time -
				job.tasks[ThreadLocalRandom.current().nextInt(job.tasks.length)].completion_time;
//		return totalIdleTime;
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
	 * With in the (k,l) server we consider a job to be complete and
	 * departed when k of its tasks are complete.  But we let the other
	 * tasks keep running.  Do we need to keep the completed jobs around until
	 * all their tasks finish (if they aren't sampled)?
	 * 
	 * @param worker
	 * @param time
	 */
	public void taskCompleted(FJWorker worker, double time) {
		//if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.ID+" completed "+time);
		FJTask task = worker.current_task;
		worker.current_task = null; // Task completed. Remove it from worker.
		task.completion_time = time;
		task.completed = true;
		task.job.num_tasks_started--;
		if (++task.job.num_tasks_completed == this.current_job.tasks.length) {
			task.job.completed = true;
			task.job.completion_time = time;
			task.job.departure_time = time;
			task.job.workerIdleTime = getWorkersWaitingTime(task.job);
//			if (task.job.num_tasks_started == 0) {
				// Completed Job and no task of it is running
				jobDepart(task.job);
				current_job = job_queue.poll();
				// if there is no current job, just clear the worker
				if (current_job == null) {
					if (FJSimulator.DEBUG) System.out.println("  no current_job");
					return;
				}

//			}
			feedWorkers(time);
		} else {
			// Job not completed, service a new task on the current worker
			serviceTask(worker, current_job.nextTask(), time);
		}
	}
	
	
	/**
	 * In the single-queue server the queue length is simply the length of the job queue.
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
