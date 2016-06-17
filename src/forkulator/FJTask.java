package forkulator;

public class FJTask {

	private static long ID_counter = 0;
	public long ID = ID_counter++;

	public double start_time = 0.0;
	public double service_time = 0.0;
	public double completion_time = 0.0;  // redundant
	public int worker = -1;
	public boolean processing = false;
	public boolean completed = false;
	
	public FJTask(IntertimeProcess service_process) {
		this.service_time = service_process.nextInterval();
	}
	
	
	
	
}
