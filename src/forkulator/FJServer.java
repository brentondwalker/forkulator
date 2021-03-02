package forkulator;

import forkulator.randomprocess.IntertimeProcess;

import java.util.ArrayList;

public abstract class FJServer {

	FJSimulator simulator = null;
	
	public int num_workers = 1;
	public int num_stages = 1;
	public FJWorker[][] workers = null;
	public FJJob current_job = null; // needs to change for multi-stage
	public ArrayList<FJJob> sampled_jobs = new ArrayList<FJJob>();
	public IntertimeProcess overhead_process = null;

	public IntertimeProcess getOverhead_process() {
		return overhead_process;
	}

	public void setOverheadProcesses(IntertimeProcess overhead_process, IntertimeProcess second_overhead_process) {
		this.overhead_process = overhead_process;
		for (int row = 0; row < workers.length; row++) {
			for (int col = 0; col < workers[row].length; col++) {
				workers[row][col].setOverheadProcesses(overhead_process, second_overhead_process);
			}
		}
	}

	/**
	 * Super constructor.
	 * 
	 * This constructor now supports multi-stage systems.
	 * 
	 * @param num_workers
	 */
	public FJServer(int num_workers, int num_stages) {
		this.num_workers = num_workers;
		this.num_stages = num_stages;
		this.workers = new FJWorker[num_stages][num_workers];
		for (int j=0; j<num_stages; j++) {
			for (int i=0; i<num_workers; i++) {
				workers[j][i] = new FJWorker(j);
			}
		}
	}
	
	/**
	 * Constructor for single-stage system
	 * 
	 * @param num_workers
	 */
	public FJServer(int num_workers) {
		this(num_workers,1);
	}
	
	
	/**
	 * The server needs a reference to the simulator so it can schedule new events.
	 * 
	 * @param simulator
	 */
	public void setSimulator(FJSimulator simulator) {
		this.simulator = simulator;
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
	public abstract void taskCompleted(FJWorker worker, double time);
	
	
	/**
	 * Return the length of the queue of jobs waiting for service.
	 * 
	 * @return
	 */
	public abstract int queueLength();
	
	
	/**
	 * Put the task on the specified worker and schedule its completion in the event queue.
	 * 
	 * @param workerId
	 * @param task
	 * @param time
	 */
	public void serviceTask(FJWorker worker, FJTask task, double time) {
	    assert(worker.current_task == null);
		if (task != null) {
			assert(task.job != null);
			if (FJSimulator.DEBUG) System.out.println("serviceTask() "+task);
			worker.handleTask(task);
			task.job.num_tasks_started++;
			task.start_time = time;
			task.processing = true;
//			// schedule the task's completion
			QTaskCompletionEvent e = new QTaskCompletionEvent(task, time + worker.serviceTask());
			simulator.addEvent(e);
		}
	}
	
	
	/**
	 * The server should call this when a job is complete and departing.
	 * This samples the stats of the job if it is marked for sampling, and
	 * prepares the job for disposal.
	 * A job object shouldn't be accessed after this is called.
	 * 
	 * The FJDataAggregator will only sample the job if it is marked for sampling, 
	 * but we call this on each job in case the aggregator is recording the
	 * experiment path or something.
	 * 
	 * @param j
	 */
	public void jobDepart(FJJob j) {
		if (simulator.data_aggregator != null) {
			simulator.data_aggregator.sample(j);
		}
		simulator.stability_aggregator.sample(j);
		j.dispose();
	}

	abstract int numJobsInQueue();

}
