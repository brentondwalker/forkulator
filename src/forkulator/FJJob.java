package forkulator;

import java.util.ArrayList;

public class FJJob implements Comparable<FJJob> {

	private static long ID_counter = 0;
	public long ID = ID_counter++;
	
	public double arrival_time = 0.0;
	public double completion_time = 0.0;
	public double departure_time = 0.0;
	public int num_tasks = 0;
	public FJTask[] tasks = null;
	private int task_index = 0;
	public boolean completed = false;
	public boolean fully_serviced = false;
	public boolean sample = false;
	
	public static ArrayList<IntertimeProcess> service_processes = null;
	
	/**
	 * Constructor
	 * 
	 * @param num_tasks
	 * @param service_process
	 * @param arrival_time
	 */
	public FJJob(int num_tasks, IntertimeProcess service_process, double arrival_time) {
		this.num_tasks = num_tasks;
		
		// in the first call set up an independent service_process for each
		// task "channel".  That is, task 1 will always come from the same
		// service process, all task 2's will etc...
		// If the number of service processes allocated before is not enough,
		// add more.
		// I am not really happy with this implicit initialization.
		if (service_processes == null) {
			service_processes = new ArrayList<IntertimeProcess>(num_tasks);
			System.err.println("service_processes.size="+service_processes.size());
			for (int i=0; i<num_tasks; i++) {
				service_processes.add(i, service_process.clone());
			}
		} else if (service_processes.size() < num_tasks) {
			service_processes.ensureCapacity(num_tasks);
			for (int i=service_processes.size(); i<num_tasks; i++) {
				service_processes.add(i, service_process.clone());
			}
		}
		
		tasks = new FJTask[this.num_tasks];
		for (int i=0; i<this.num_tasks; i++) {
			tasks[i] = new FJTask(service_processes.get(i), arrival_time, this);
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
	 * clean up the object to hopefully make things easier for the garbage collector
	 */
	public void dispose() {
		if (! this.sample) {
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
		if (ID < o.ID) {
			return -1;
		} else if (ID > o.ID) {
			return 1;
		}
		return 0;
	}
}
