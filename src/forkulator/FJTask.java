package forkulator;

import forkulator.randomprocess.IntertimeProcess;

public class FJTask {
	
	public double start_time = 0.0;
	public double service_time = 0.0;
	public double service_scale_factor = 1.0;
	public double completion_time = 0.0;  // redundant
	public FJWorker worker = null;
	public boolean processing = false;
	public boolean completed = false;
	public FJJob job = null;
	public IntertimeProcess service_process = null;
	
	// this is assigned and used by FJPathLogger to keep track of the sequence of task arrivals
	public int path_log_id = -1;
	
	// if we add data location awareness to the tasks, this will represent
	// which worker holds the data needed for this task
	// for now assume there is no redundancy in the placement of data.
	public int data_host = -1;
	
    /**
     * Constuctor
     * 
     * @param service_process
     * @param arrival_time
     * @param job
     */
    public FJTask(IntertimeProcess service_process, double arrival_time, FJJob job) {
        this(service_process, arrival_time, job, 1.0);
    }

    /**
     * Constructor
     * 
     * @param service_process
     * @param arrival_time
     * @param job
     * @param service_scale_factor
     */
    public FJTask(IntertimeProcess service_process, double arrival_time, FJJob job, double service_scale_factor) {
        this.service_process = service_process;
        this.service_scale_factor = service_scale_factor;
        this.service_time = service_process.nextInterval(arrival_time) * service_scale_factor;
        this.job = job;
    }
	
	
	/**
	 * Generate a new service time from the service time process.
	 * This is useful for multi-stage systems with independent stages.
	 */
	public void resampleServiceTime() {
		service_time = service_process.nextInterval();
	}
	
	
	/**
	 * Generate a new service time from the service time process.
	 * This is useful for multi-stage systems with independent stages.
	 * 
	 * This version passes the arrival time to the service time process,
	 * in case you are using leaky buckets or something.
	 */
	public void resampleServiceTime(double arrival_time) {
		service_time = service_process.nextInterval(arrival_time);
	}

}
