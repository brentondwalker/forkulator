package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import forkulator.randomprocess.IntervalPartition;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Base class for partitioning jobs
 * 
 * @author stefan
 *
 */
abstract public class FJPartitionJob extends FJJob {
    // Class array with constructor parameter types
    static Class[] constructor_parameters = new Class[] {int.class, int.class,
            IntertimeProcess.class, IntervalPartition.class, double.class};

    /**
     * instance
     * Empty constructor which should be used to create an object with the purpose to call
     * createNewInstance. The alternative would be to create a variable with the constructor as
     * content which makes error checking more complex.
     */
    public FJPartitionJob() {
    }

    /**
     * Constructor
     *
     * @param num_tasks
     * @param num_workers
     * @param service_process
     * @param arrival_time
     */
    public FJPartitionJob(int num_tasks, int num_workers, IntertimeProcess service_process, IntervalPartition job_partition_process, double arrival_time) {
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

    abstract FJPartitionJob createNewInstance(int num_tasks, int num_workers,
                                              IntertimeProcess service_process,
                                              IntervalPartition job_partition_process,
                                              double arrival_time);
}
