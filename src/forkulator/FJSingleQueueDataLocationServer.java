package forkulator;

import java.util.LinkedList;
import java.util.Queue;

public class FJSingleQueueDataLocationServer extends FJServer {

    /**
     * This server models a dataset distributed amongst the workers.
     * If a task is scheduled on a worker that does not hold the task's
     * data, then the data must be copied over, resulting in a time penalty.
     * We assume that the service time and the penalty of copying is
     * proportional to the data size.  Therefore the penalty is expressed
     * as a factor that will multiply the service time in non-local
     * execution cases.
     */
    public double data_location_penalty = 1.0;
    
    
    /**
     * This type of server has a single job queue, and tasks are drawn from
     * the job at the front of the queue until the job is fully serviced.
     * 
     * The way I implemented this, I put the current "head" job in current_job,
     * and the jobs in the queue are the ones not serviced yet.
     */
    public Queue<FJJob> job_queue = new LinkedList<FJJob>();
    public FJJob current_job = null;
    
    
    /**
     * Constructor
     * 
     * Since we allocate the single job_queue above, there is nothing
     * special for this constructor to do except call super().
     * 
     * @param num_workers
     */
    public FJSingleQueueDataLocationServer(int num_workers, double data_location_penalty) {
        super(num_workers);
        this.data_location_penalty = data_location_penalty;
    }
    
    
    /**
     * Check for any idle workers and try to put a task on them.
     * 
     * Since this server is data location aware, we first check if
     * there's a task whose data is located on the vacant worker.
     * 
     * @param time
     */
    public void feedWorkers(double time) {
        // check for idle workers
        for (int i=0; i<num_workers; i++) {

            if (workers[0][i].current_task == null) {
                //if (FJSimulator.DEBUG) System.out.println("feedWorkers() worker is vacant: "+i);

                while (workers[0][i].current_task == null) {

                    // if there is no current job, just return
                    if (current_job == null) return;

                    // check if this job has an unserviced task whose data is on this worker
                    // TODO: this logic would be more consistent to put in FJJob
                    for (int j=0; j<current_job.tasks.length; j++) {
                        FJTask tt = current_job.tasks[j];
                        if (tt.data_host == i) {
                            if ((! tt.processing) && (! tt.completed)) {
                                //if (FJSimulator.DEBUG) System.out.println("feedWorkers() found a macthed spot on worker "+i);
                                serviceTask(workers[0][i], tt, time);
                            }
                        }
                    }

                    // otherwise service the next task
                    // TODO: if multiple workers can be free at the same time,
                    // then this is not optimal.  We might pick a task that would be better
                    // to run on another vacant worker.
                    // Might be better to loop over the remaining tasks and pick the best worker??
                    // no, not really.  Neither way is foolproof.
                    if (workers[0][i].current_task == null) {
                        for (int j=0; j<current_job.tasks.length; j++) {
                            FJTask tt = current_job.tasks[j];
                            if ((! tt.processing) && (! tt.completed)) {
                                // since this isn't the task's data location, there's a penalty in service time
                                // for starters, assume the penalty is equal to the service time
                                //if (FJSimulator.DEBUG) System.out.println("feedWorkers() settled for a spot on worker "+i);
                                tt.service_time *= data_location_penalty;
                                serviceTask(workers[0][i], tt, time);
                                 break;
                            }
                        }
                    }

                    // if there were no tasks left, mark the job as serviced and dequeue
                    if (workers[0][i].current_task == null) {
                        //if (FJSimulator.DEBUG) System.out.println("feedWorkers() job is fully serviced");
                        current_job.fully_serviced = true;
                    }

                    // if the current job is exhausted, grab a new one (or null)
                    if (current_job.fully_serviced) {
                        //if (FJSimulator.DEBUG) System.out.println("feedWorkers() grab a new job");
                        current_job = job_queue.poll();
                    }
                }
                //if (FJSimulator.DEBUG) System.out.println("feedWorkers() done with worker "+i);
            }
        }
    }
    
    /**
     * Enqueue a new job.
     * 
     * This server type has a single job queue, and tasks are pulled from the job
     * at the head of the queue until they're exhausted.
     * 
     * @param job
     * @param sample
     */
    public void enqueJob(FJJob job, boolean sample) {
        //if (FJSimulator.DEBUG) System.out.println("enqueJob() "+job.arrival_time);

        // only keep a reference to the job if the simulator tells us to
        job.setSample(sample);
        
        if (current_job == null) {
            current_job = job;
            //if (FJSimulator.DEBUG) System.out.println("  current job was null");
            feedWorkers(job.arrival_time);
        } else {
            job_queue.add(job);
            //if (FJSimulator.DEBUG) System.out.println("  current job was full, so I queued this one");
        }
    }
    
    
    /**
     * Handle a task completion event.  Remove the task from its worker, and
     * give the worker a new task, if available.
     * 
     * @param workerId
     * @param time
     */
    public void taskCompleted(FJWorker worker, double time) {
        //if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.ID+" completed "+time);
        FJTask task = worker.current_task;
        task.completion_time = time;
        task.completed = true;

        // check if this task was the last one of a job
        //TODO: this could be more efficient
        boolean compl = true;
        for (FJTask t : task.job.tasks) {
            compl = compl && t.completed;
        }
        task.job.completed = compl;

        if (task.job.completed) {
            // it is the last, record the completion time
            task.job.completion_time = time;
            
            // for this type of server it is also the departure time
            task.job.departure_time = time;
            
            // sample and dispose of the job
            jobDepart(task.job);
        }
        
        // clear the worker and rely on feedWorkers() to put a new task on it
        worker.current_task = null;
        
        // put a new task on the worker
        //serviceTask(worker, current_job.nextTask(), time);
        this.feedWorkers(time);
        
    }


    /**
     * In the single-queue server the queue length is simply the length of the job queue.
     */
    @Override
    public int queueLength() {
        return this.job_queue.size();
    }
    
    
}
