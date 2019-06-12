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
     * Ensure that: All tasks of current job must get serviced before the next job.
     * This complicates things if there are multiple idle servers.  Suppose there are
     * two idle servers, 1, 2, and the current job only has a task that wants to run
     * on server 2.  Ideally we would assign that task to server 2, and a task from the
     * next job to server 1.  But if we assign tasks to servers in order, then we
     * assign that task to server 1, and a task from the next job to server 2.   
     * 
     * The solution is to loop over the waiting tasks and see if they can find a home.
     * If not, assign them to any idle server.
     * This will work under the assumption that each job contains at least one task
     * for each server.  In that case the next job in line has at least enough tasks
     * to feed every server, and the stipulation above means that we can never look
     * more than one job deep into the queue.
     * 
     * In the single-queue system, if there are tasks waiting, then only one server
     * can become idle at a time.  If the task queue is empty, then when a new job
     * arrives we can safely iterate over the idle workers and see if the job
     * contains any tasks for them.  In the normal situation each job will contain
     * one task for each worker.
     * 
     * @param time
     */
    public void feedWorkers(double time) {
        // if there is no current job, just return
        if (current_job == null && job_queue.isEmpty()) return;
        if (current_job.fully_serviced) {
            //if (FJSimulator.DEBUG) System.out.println("feedWorkers() grab a new job");
            current_job = job_queue.poll();
        }
        
        int num_idle_workers = 0;
        for (int i=0; i<num_workers; i++) {
            if (workers[0][i].current_task == null) num_idle_workers++;
        }
        
        while (num_idle_workers > 0 && current_job!=null) {
        
            boolean task_assigned = false;
            boolean tasks_remaining = false;

            // first pass: try to match a task to a worker holding its data
            for (int j=0; j<current_job.tasks.length; j++) {
                FJTask tt = current_job.tasks[j];
                
                if (!tt.processing && !tt.completed) {
                    tasks_remaining = true;

                    // try and find an idle server hosting the data for this task
                    // check for idle workers
                    for (int i=0; i<num_workers; i++) {

                        if (workers[0][i].current_task == null) {
                            //if (FJSimulator.DEBUG) System.out.println("feedWorkers() worker is vacant: "+i);
                            if (tt.data_host == i) {
                                task_assigned = true;
                                if (FJSimulator.DEBUG) System.out.println("feedWorkers() assigning task "+j+" to worker "+i+" holding its data");
                                serviceTask(workers[0][i], tt, time);
                                num_idle_workers--;
                                break;
                            }
                        }
                    }
                }
            }
            
            // optional pass 2: we iterated all tasks and all idle workers and didn't
            // find a match.  just assign the first task to the first available worker.
            if (tasks_remaining && !task_assigned) {
                for (int j=0; j<current_job.tasks.length; j++) {
                    FJTask tt = current_job.tasks[j];
                    if (!tt.processing && !tt.completed) {
                        for (int i=0; i<num_workers; i++) {
                            if (workers[0][i].current_task == null) {
                                task_assigned = true;
                                if (FJSimulator.DEBUG) System.out.println("feedWorkers() assigning task "+j+" to worker "+i+"");
                                tt.service_time *= (1.0 + data_location_penalty);
                                serviceTask(workers[0][i], tt, time);
                                num_idle_workers--;
                                break;
                            }
                        }
                    }
                }
            }
            
            // if there were no tasks left, mark the job as serviced and dequeue
            if (! tasks_remaining) {
                //if (FJSimulator.DEBUG) System.out.println("feedWorkers() job is fully serviced");
                current_job.fully_serviced = true;
            }
            
            // if the current job is exhausted, grab a new one (or null)
            if (current_job.fully_serviced) {
                //if (FJSimulator.DEBUG) System.out.println("feedWorkers() grab a new job");
                current_job = job_queue.poll();
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

    @Override
    int numJobsInQueue() {
        return job_queue.size();
    }
    
    
}
