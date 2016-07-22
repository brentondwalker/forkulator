package forkulator;

public class FJTask {

	private static long ID_counter = 0;
	public long ID = ID_counter++;

	public double start_time = 0.0;
	public double service_time = 0.0;
	public double completion_time = 0.0;  // redundant
	public FJWorker worker = null;
	public boolean processing = false;
	public boolean completed = false;
	public FJJob job = null;
	
	public FJTask(IntertimeProcess service_process, double arrival_time, FJJob job) {
		this.service_time = service_process.nextInterval(arrival_time);
		this.job = job;
	}
	
	
	
	
}
