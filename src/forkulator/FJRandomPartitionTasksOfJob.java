package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import forkulator.randomprocess.IntervalPartition;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Math.abs;


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
public class FJRandomPartitionTasksOfJob extends FJPartitionJob {

    FJRandomPartitionTasksOfJob() {};

    /**
     * Constructor
     *
     * @param num_tasks
     * @param num_workers
     * @param service_process
     * @param arrival_time
     */
    public FJRandomPartitionTasksOfJob(int num_tasks, int num_workers, IntertimeProcess service_process, IntervalPartition job_partition_process, double arrival_time) {
        this.num_tasks = num_tasks;
        int splitVal = num_tasks / num_workers;

        tasks = new FJTask[this.num_tasks];
        this.num_tasks = num_tasks;
        this.job_service_time = 0.0;
        double service_sum = 0.0;

        for (int i=0; i<num_workers; i++) {
            double bigTaskTime = service_process.nextInterval();
            IntertimeProcess task_service_process =
                job_partition_process.getNewPartition(bigTaskTime, splitVal);
            for (int j=0; j<splitVal; j++) {
                if (job_partition_process.independentSamples()) {
                    bigTaskTime = service_process.nextInterval();
                    task_service_process =
                            job_partition_process.getNewPartition(bigTaskTime, splitVal);
                }
                tasks[i*splitVal+j] = new FJTask(task_service_process, arrival_time, this);
                tasks[i*splitVal+j].data_host = rand.nextInt(num_workers);
                // Calculating job service time from task service times to calculate the right value
                // with and without independent samples
                this.job_service_time += tasks[i*splitVal+j].service_time;
            }
        }
    }

    /**
     * Shuffles an array. Can be used to reduce the correlation between tasks which are created
     * from splitting multiple bigger tasks.
     * @param ar The array which gets randomized
     */
    void shuffleArray(FJTask[] ar)
    {
        Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--)
        {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            FJTask a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    @Override
    FJPartitionJob createNewInstance(int num_tasks, int num_workers,
                                     IntertimeProcess service_process,
                                     IntervalPartition job_partition_process, double arrival_time) {
        return new FJRandomPartitionTasksOfJob(num_tasks, num_workers, service_process,
                job_partition_process, arrival_time);
    }
}
