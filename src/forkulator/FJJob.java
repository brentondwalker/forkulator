package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import java.util.Random;


public class FJJob implements Comparable<FJJob> {

    // essential data about the job
    public double arrival_time = 0.0;
    public double completion_time = 0.0;
    public double departure_time = 0.0;

    // the job's tasks
    public int num_tasks = 0;
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
    
    private Random rand = new Random();
    

    /**
     * Constructor
     * 
     * @param num_tasks
     * @param service_process
     * @param arrival_time
     */
    public FJJob(int num_tasks, int num_workers, IntertimeProcess service_process, double arrival_time) {
        this.num_tasks = num_tasks;

        tasks = new FJTask[this.num_tasks];
        for (int i=0; i<this.num_tasks; i++) {
            tasks[i] = new FJTask(service_process, arrival_time, this);
            // TODO: this assumes the number of tasks equals the number of servers
            //tasks[i].data_host = i;
            tasks[i].data_host = rand.nextInt(num_workers);
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
