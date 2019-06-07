package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import forkulator.randomprocess.IntervalPartition;


/**
 * This class models a job where the total service time of the job is drawn from
 * some random distribution, and the service times of the tasks are a random
 * sub-division of that time.
 * 
 * In this case the task service times are not iid, namely they are not independent.
 * 
 * @author brenton
 *
 */
public class FJRandomTaskPartitionJob extends FJJob {

    /*
     * This class treats the task_service_process as generating task service
     * times, and also holds an IntervalPartition process that divides up
     * the tasks into smaller tasks.
     * 
     * XXX: The other FJJob classes could all be replaced by this one.
     * - when task_division_factor=1 then this is the same as FJIndependentTaskJob
     * - when num_big_tasks=1 then this is the same as FJRandomPartitionJob
     *       
     */
    IntervalPartition task_partition_process = null;
    
    int num_big_tasks = 0;
    int task_division_factor = 1;
    
    /**
     * Constructor
     * 
     * @param num_tasks
     * @param num_workers
     * @param service_process
     * @param arrival_time
     */
    public FJRandomTaskPartitionJob(int num_big_tasks, int num_workers, int task_division_factor,  IntertimeProcess service_process, IntervalPartition task_partition_process, double arrival_time) {
        this.num_big_tasks = num_big_tasks;
        this.task_division_factor = task_division_factor;
        this.num_tasks = num_big_tasks * task_division_factor;

        tasks = new FJTask[this.num_tasks];
        this.job_service_time = 0.0;
        this.task_partition_process = task_partition_process;

        //double[] tstmp = new double[this.num_tasks];
        //double service_sum = 0.0;
        
        for (int ti=0; ti<num_big_tasks; ti++) {
            this.task_service_process = task_partition_process.getNewPartition(service_process.nextInterval()*num_workers, this.task_division_factor);

            for (int i=0; i<this.num_tasks; i++) {
                if (task_partition_process.independentSamples()) {
                    tasks[ti*task_division_factor + i] = new FJTask(task_partition_process.getNewPartition(service_process.nextInterval()*num_workers, this.task_division_factor), arrival_time, this);
                } else {
                    tasks[ti*task_division_factor + i] = new FJTask(task_service_process, arrival_time, this);
                }
                this.job_service_time += tasks[ti*task_division_factor + i].service_time;
                
                tasks[i].data_host = rand.nextInt(num_workers);
            }
        }
    }

}
