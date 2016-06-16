package forkulator;

import java.util.Random;

public class FJTask {

	private static Random rand = new Random();

	private static long ID_counter = 0;
	public long ID = ID_counter++;

	public double start_time = 0.0;
	public double service_time = 0.0;
	public double completion_time = 0.0;  // redundant
	public int worker = -1;
	public boolean processing = false;
	public boolean completed = false;
	
	public FJTask(int num_tasks, double service_rate) {
		if (FJJob.ERLANG) {
			// this version gives Erlang service times
			double p = 1.0;
			for (int i=0; i<num_tasks; i++) {
				p *= rand.nextDouble();
			}
			this.service_time = -Math.log(p)/service_rate;
		} else {
			// exponential service times
			this.service_time = -Math.log(rand.nextDouble())/service_rate;
		}
	}
	
	
	
	
}
