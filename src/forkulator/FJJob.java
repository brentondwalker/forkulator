package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import java.util.Random;

/**
 * In order to support jobs with iid tasks and also jobs where the service
 * time applies to the job, and the service times of the tasks are subdivisions
 * of the service time of the job (and other non-iid cases), we make the FJJob
 * an abstract class.  I am not making it an interface, because there is still
 * common functionality between FJJob types that will be convenient to implement
 * here, such as the Comparable interface.
 * 
 * @author brenton
 *
 */
public abstract class FJJob implements Comparable<FJJob> {

    // essential data about the job
    public double arrival_time = 0.0;
    public double completion_time = 0.0;
    public double departure_time = 0.0;

    // the job's tasks
    public int num_tasks = 0;
    // makes it unnecessary to iterate through the tasks to get this number.
    // could be merged with (@link completed)
    public int num_tasks_completed = 0;
    // prevent iterating through tasks to check if tasks are running.
    public int num_tasks_running = 0;
    public FJTask[] tasks = null;

    // index to keep track of which tasks have been pulled out for service
    private int task_index = 0;

    // set to true when a worker finishes a job
    public boolean completed = false;

    // in a multi-stage system, set to true when 
    public boolean fully_serviced = false;

    // set to true if the job's statistics should be sampled when it departs
    public boolean sample = false;

    // this is assigned and used by FJPathLogger to keep track of the sequence of job arrivals
    public int path_log_id = -1;
    
    // the random process giving service times of jobs
    // this would be used in models where the job service times are a random process and
    // the tasks service times are a subdivision of the job service time
    public IntertimeProcess job_service_process = null;
    public double job_service_time = 0;
    
    // the random process giving service times to this job's tasks
    // this is passed to the FJTask constructor.
    public IntertimeProcess task_service_process = null;
    
    // at the moment this is only used to assign the data hosts of the tasks
    protected static Random rand = new Random();
    

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
     * Clean up the object to hopefully make things easier for the garbage collector.
     * 
     * This also feeds the sampled jobs to the data_aggregator before destroying the job.
     * 
     * Having pathlog set prevents this from doing anything.
     */
    public void dispose() {
        for (FJTask t : this.tasks) {
            t.job = null;
        }
        this.tasks = null;
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
