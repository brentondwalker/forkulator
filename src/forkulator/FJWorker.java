package forkulator;

import forkulator.randomprocess.IntertimeProcess;

import java.util.LinkedList;
import java.util.Queue;

public class FJWorker {

	public double lastTimeCompleted = 0;
		
	public int stage = 0;
	
	public FJTask current_task = null;
	
	public Queue<FJTask> queue = null;

	public Queue<Double> last_job_ids = new LinkedList<>();

	public IntertimeProcess first_overhead_process = null;

	public IntertimeProcess second_overhead_process = null;

	
	/**
	 * Constructor.
	 * 
	 * For book keeping convenience, the worker knows what stage it is
	 * in.  This really only applies to multi-stage systems.
	 * 
	 * @param stage
	 */
	public FJWorker(int stage) {
		this.stage = stage;
	}
	
	/**
	 * Constructor for single-stage systems.  The stage will default to 0.
	 */
	public FJWorker() {
		this(0);
	}

	public void handleTask(FJTask task) {
		task.worker = this;
		this.current_task = task;
		last_job_ids.add((double) task.job.job_id);
	}

	protected boolean isFirstTask() {
		for(Double e: last_job_ids)
			if(e.compareTo((double) this.current_task.job.job_id) == 0) {
				return false;
			}
		return true;
	}

	/**
	 * Services task by setting the overhead in the current task and returns the service time with overhead.
	 * @return Service time with overhead
	 */
	public double serviceTask() {
		assert (this.current_task != null);
		// If overhead:time is bigger than 0.0, assume that the overhead is already calculated
		if (first_overhead_process != null && this.current_task.overhead_time <= 0.0) {
			if (second_overhead_process != null) {
				if (isFirstTask())
					this.current_task.overhead_time = first_overhead_process.nextInterval();
				else
					this.current_task.overhead_time = first_overhead_process.nextInterval();
			} else
				this.current_task.overhead_time = first_overhead_process.nextInterval();
		}
		return this.current_task.overhead_time + this.current_task.service_time;
	}

	public void setOverheadProcesses(IntertimeProcess overhead_processs, IntertimeProcess second_overhead_process) {
		this.first_overhead_process = overhead_processs;
		this.second_overhead_process = second_overhead_process;
	}
}
