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
	
	public FJTask(double service_rate) {
		this.service_time = -Math.log(rand.nextDouble())/service_rate;
	}
	
	
	
	
}
