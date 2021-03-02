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
public class FJRandomPartitionJobUnscaled extends FJPartitionJob {

    /*
     * Instead of keeping track of a task_service_process, this class treats the
     * task_service_process as generating job service times, and also holds a
     * IntervalPartition process that divides up the job service times.
     */
    IntervalPartition job_partition_process = null;

    FJRandomPartitionJobUnscaled() {}
    /**
     * Constructor
     *
     * @param num_tasks
     * @param num_workers
     * @param service_process
     * @param arrival_time
     */
    public FJRandomPartitionJobUnscaled(int num_tasks, int num_workers, IntertimeProcess service_process, IntervalPartition job_partition_process, double arrival_time) {
        this.num_tasks = num_tasks;

        tasks = new FJTask[this.num_tasks];
        this.num_tasks = num_tasks;
        this.job_service_time = service_process.nextInterval();
        this.job_partition_process = job_partition_process;
        this.task_service_process = job_partition_process.getNewPartition(job_service_time, this.num_tasks);

        //double[] tstmp = new double[this.num_tasks];
        //double service_sum = 0.0;
        
        for (int i=0; i<this.num_tasks; i++) {
            if (job_partition_process.independentSamples()) {
                tasks[i] = new FJTask(job_partition_process.getNewPartition(service_process.nextInterval(), this.num_tasks), arrival_time, this);
            } else {
                tasks[i] = new FJTask(task_service_process, arrival_time, this);
            }
            // TODO: this assumes the number of tasks equals the number of servers
            //tasks[i].data_host = i;
            tasks[i].data_host = rand.nextInt(num_workers);
            //tstmp[i] = tasks[i].service_time;
            //service_sum += tstmp[i];
        }
        
        //System.out.println("task service times: "+Arrays.toString(tstmp)+"  "+service_sum);
    }

    @Override
    FJPartitionJob createNewInstance(int num_tasks, int num_workers,
                                     IntertimeProcess service_process,
                                     IntervalPartition job_partition_process, double arrival_time) {
        return new FJRandomPartitionJobUnscaled(num_tasks, num_workers, service_process,
                job_partition_process, arrival_time);
    }
}
