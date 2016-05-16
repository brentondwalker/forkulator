package forkulator;

import java.util.ArrayList;

public abstract class FJServer {

	static FJSimulator simulator = null;
	
	public int num_workers = 0;
	public FJWorker[] workers = null;
	public FJJob current_job = null;
	public ArrayList<FJJob> sampled_jobs = new ArrayList<FJJob>();
	
	
	/**
	 * Super constructor.
	 * 
	 * @param num_workers
	 */
	public FJServer(int num_workers) {
		this.num_workers = num_workers;
		this.workers = new FJWorker[num_workers];
		for (int i=0; i<num_workers; i++) {
			workers[i] = new FJWorker();
		}
	}
	
	
	/**
	 * The server needs a reference to the simulator so it can schedule new events.
	 * 
	 * @param simulator
	 */
	public static void setSimulator(FJSimulator simulator) {
		FJServer.simulator = simulator;
	}
	
	
	/**
	 * Check for any idle workers and try to put a task on them.  This depends
	 * on the type of queuing system we are working with.
	 * 
	 * @param time
	 */
	public abstract void feedWorkers(double time);
	
	
	/**
	 * Enqueue a new job.  How this is done depends on they type of queuing
	 * system we are modeling.
	 * 
	 * @param job
	 * @param sample
	 */
	public abstract void enqueJob(FJJob job, boolean sample);
	
	
	/**
	 * Handle a task completion event.  Remove the task from its worker, and
	 * give the worker a new task, if available.
	 * 
	 * @param workerId
	 * @param time
	 */
	public abstract void taskCompleted(int workerId, double time);
	
	
	/**
	 * Put the task on the specified worker and schedule its completion in the event queue.
	 * 
	 * @param workerId
	 * @param task
	 * @param time
	 */
	public void serviceTask(int workerId, FJTask task, double time) {
		if (FJSimulator.DEBUG) System.out.println("serviceTask() "+task.ID);
		workers[workerId].current_task = task;
		task.worker = workerId;
		task.start_time = time;
		task.processing = true;
		
		// schedule the task's completion
		QTaskCompletionEvent e = new QTaskCompletionEvent(task, time + task.service_time);
		simulator.addEvent(e);
	}

}
