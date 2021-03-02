package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import forkulator.randomprocess.IntervalPartition;


/**
 * This class models a job where the tasks have iid service times.
 * 
 * In this case, the total service required for the job is the sum of the service
 * times of the tasks, which will probably have a different distribution.
 * For example if the task service times are exponential, then the total job
 * service time will be Erlang-k.
 * 
 * @author brenton
 *
 */
public class FJIndependentTaskJob extends FJPartitionJob {

    FJIndependentTaskJob(){}

    /**
     * Constructor
     * 
     * @param num_tasks
     * @param num_workers
     * @param service_process
     * @param arrival_time
     */
    public FJIndependentTaskJob(int num_tasks, int num_workers, IntertimeProcess service_process, double arrival_time) {
        this.num_tasks = num_tasks;

        tasks = new FJTask[this.num_tasks];
        for (int i=0; i<this.num_tasks; i++) {
            long t1 = System.currentTimeMillis();
            tasks[i] = new FJTask(service_process, arrival_time, this);
            // TODO: this assumes the number of tasks equals the number of servers
            //tasks[i].data_host = i;
            tasks[i].data_host = rand.nextInt(num_workers);
        }
    }

    @Override
    FJPartitionJob createNewInstance(int num_tasks, int num_workers,
                                     IntertimeProcess service_process,
                                     IntervalPartition job_partition_process,double arrival_time) {
        return new FJIndependentTaskJob(num_tasks, num_workers, service_process, arrival_time);
    }
}
