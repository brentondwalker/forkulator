package forkulator;

import java.util.ArrayList;

public class FJJob {

	private static long ID_counter = 0;
	public long ID = ID_counter++;
	
	public double arrival_time = 0.0;
	public double completion_time = 0.0;
	public int num_tasks = 0;
	public FJTask[] tasks = null;
	private int task_index = 0;
	public boolean complete = false;
	
	public FJJob(int num_tasks, double service_rate) {
		this.num_tasks = num_tasks;
		tasks = new FJTask[num_tasks];
		for (int i=0; i<num_tasks; i++) {
			tasks[i] = new FJTask(service_rate);
		}
	}
	
	public FJTask nextTask() {
		task_index++;
		if (task_index == num_tasks) complete = true;
		return tasks[task_index-1];
	}
}
