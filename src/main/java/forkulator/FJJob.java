package forkulator;

public class FJJob implements Comparable<FJJob> {

	public double arrival_time = 0.0;
	public double completion_time = 0.0;
	public double departure_time = 0.0;
	public int num_tasks = 0;
	public FJTask[] tasks = null;
	private int task_index = 0;
	public boolean completed = false;
	public boolean fully_serviced = false;
	public boolean sample = false;
	public boolean pathlog = false;
		
	/**
	 * Constructor
	 * 
	 * @param num_tasks
	 * @param service_process
	 * @param arrival_time
	 */
	public FJJob(int num_tasks, IntertimeProcess service_process, double arrival_time) {
		this.num_tasks = num_tasks;
		
		tasks = new FJTask[this.num_tasks];
		for (int i=0; i<this.num_tasks; i++) {
			tasks[i] = new FJTask(service_process, arrival_time, this);
		}
	}
	
	/**
	 * Set a flag that records whether or not this job is
	 * part of the sample set.
	 * 
	 * @param s
	 */
	public void setSample(boolean s) {
		this.sample = s;
	}
	
	/**
	 * Set a flag that records whether or not this job is
	 * being saved to record the experiment path.
	 * 
	 * @param s
	 */
	public void setPatlog(boolean s) {
		this.pathlog = s;
	}
	
	/**
	 * 
	 * @return
	 */
	public FJTask nextTask() {
		task_index++;
		if (task_index == num_tasks) fully_serviced = true;
		if (task_index > num_tasks) return null;
		return tasks[task_index-1];
	}
	
	/**
	 * Clean up the object to hopefully make things easier for the garbage collector.
	 * 
	 * This also feeds the sampled jobs to the data_aggregator before destroying the job.
	 * 
	 * Having pathlog set prevents this from doing anything.
	 */
	public void dispose() {
		if (! this.pathlog) {
			for (FJTask t : this.tasks) {
				t.job = null;
			}
			this.tasks = null;
		}
	}
	
	
	/**
	 * In order to support thinning/resequencing it is convenient
	 * to sometimes store sets of jobs in sorted order, one way to
	 * do that is to implement Comparable.
	 */
	@Override
	public int compareTo(FJJob o) {
		if (arrival_time < o.arrival_time) {
			return -1;
		} else if (arrival_time > o.arrival_time) {
			return 1;
		}
		return 0;
	}
}
