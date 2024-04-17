package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import forkulator.randomprocess.IntervalPartition;


/**
 * This class models a job where the job has has a service-time scale factor drawn
 * from some distribution, and the tasks are i.i.d. drawn from another distribution.
 * All the tasks in the job are scaled by the job's scale factor.
 * 
 * Why?:
 * When tasks are i.i.d., and the total job service is just the sum of the task service
 * times, then as parallelism (k) increases, the relative variance of the service required
 * by each job disappears.  The jobs effectively become constant sized, which is not
 * realistic.
 * OTOH, with the random partition approach we tried before, where you choose the job size
 * first, either constant or from a random distribution, and then randomly partition that
 * interval, you almost always end up with correlated task service times, which makes
 * analysis impossible.
 * With this approach the tasks service times within a job will be correlated, but in a
 * known way.  And the distribution of the total service times of the job will scale with
 * the degree of parallelism.
 * This is based on a suggestion from Almut Burchard at the Dagstuhl NWC workshop.
 * 
 * @author brenton
 *
 */
public class FJGrandCanonicalTaskScaledJob extends FJJob {
    
    /**
     * Constructor
     * 
     * @param num_tasks
     * @param num_workers
     * @param service_process
     * @param arrival_time
     */
    public FJGrandCanonicalTaskScaledJob(int num_tasks, int num_workers, IntertimeProcess task_service_process, IntertimeProcess job_scale_process, double arrival_time) {
        this.num_tasks = num_tasks;

        tasks = new FJTask[this.num_tasks];
        this.num_tasks = num_tasks;
        this.job_service_scale_factor = job_scale_process.nextInterval();
        this.task_service_process = task_service_process;

        for (int i=0; i<this.num_tasks; i++) {
            tasks[i] = new FJTask(task_service_process, arrival_time, this, job_service_scale_factor);
            tasks[i].data_host = rand.nextInt(num_workers);
        }
    }

}
