package forkulator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;


public class FJHalfwaySplitMergeServer extends FJServer {

    /**
     * This server is like SplitMerge, except when a job gets serviced, it
     * only uses half of the available servers.  The tasks are early-assigned 
     * to workers, so even within a job, it's not like single queue.
     * 
     * Debatable: when a job is serviced, should it get half of what's available
     * at that moment, or do we have fixed job allocations: one with k/2 workers,
     * one with k/4 workers, one with k/8,...?
     * Counterintuitive, but the first option will lead to jobs getting assigned
     * less than or equal numbers of servers compared to the second.
     * 
     * The main point is that jobs can overtake each other.  An outlier task
     * can't back up the whole system.  The sacrifice is that the worker pool
     * appears *at best* half as big as it really is.  But OTOH, as long
     * as there are jobs in the queue (the server is heavily loaded) the
     * workers will be fully utilized, except within a job.
     * 
     * Hey, it's just an idea.
     */
    public Queue<FJJob> job_queue = new LinkedList<FJJob>();

    
    /**
     * This type of server has a separate queue for each worker.
     */
    
    /*
     * A structure to hold the currently active jobs.
     * Job 1 gets k/2 workers
     * Job 2 gets k/4 workers
     * Job 3 gets k/8 workers
     * ... etc
     * 
     * If the job in position 1 finishes, then a new job can become job 1.
     */
    private int maxActiveJobs = 0;
    private Vector<Vector<Integer>> jobWorkers;
    private FJJob[] activeJobs;
    private int[] workerJobPosition;
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJHalfwaySplitMergeServer(int num_workers) {
        super(num_workers);
        System.out.println("FJHalfwaySplitMergeServer()");
        
        for (int i=0; i<num_workers; i++) {
            workers[0][i].queue = new LinkedList<FJTask>();
        }
        
        // figure out how many times we can divide num_workers in half
        maxActiveJobs = 0;
        int remaining_workers = num_workers;
        while (remaining_workers > 0) {
            if (remaining_workers == 1) {
                remaining_workers = 0;
            } else {
                remaining_workers = remaining_workers - remaining_workers/2;
            }
            maxActiveJobs++;
        }
        
        // For each job position, jobWorkers will contain a list of which workers service
        // that job, and activeJobs will contain a list of the jobs in each job position.
        activeJobs = new FJJob[maxActiveJobs];
        jobWorkers = new Vector<Vector<Integer>>(maxActiveJobs);
        workerJobPosition = new int[num_workers];
        remaining_workers = num_workers;
        int worker_index = 0;
        for (int i=0; i<maxActiveJobs; i++) {
            int w = (remaining_workers==1) ? 1 : remaining_workers/2;
            jobWorkers.add(i, new Vector<Integer>(w));
            for (int j=0; j<w; j++) {
                workerJobPosition[worker_index] = i;
                jobWorkers.get(i).add(j, worker_index++);
                System.out.println("FJHalfwaySplitMergeServer: jobWorkers["+i+"]["+j+"] = "+(worker_index-1));
            }
            remaining_workers -= w;
        }
    }
    
    
    /**
     * Check for any idle workers and try to put a task on them.
     * 
     * The feedWorkers/ServiceJob cycle will become like recursion in this server type.
     * 
     * @param time
     */
    public void feedWorkers(double time) {
        
        
        // check for idle workers
        // in this server type we iterate over the job positions
        for (int jp=0; jp<maxActiveJobs; jp++) {
            // and within each job position, iterate over the workers
            boolean all_idle = true;
            for (int w : jobWorkers.get(jp)) {
                if (workers[0][w].current_task == null) {
                    // if the worker is idle, pull the next task (or null) from its queue
                    serviceTask(workers[0][w], workers[0][w].queue.poll(), time);
                }
                
                // check if a task is being serviced now
                if (workers[0][w].current_task != null) {
                    all_idle = false;
                }
            }
            
            // if all the workers are idle it means we have completed the current
            // batch of tasks.  Grab the next job and enqueue it
            if (all_idle) {
                ServiceJob(this.job_queue.poll(), jp, time);
            }
        }
    }
    
    
    /**
     * Enqueue a new job.
     * 
     * This type of server has a separate queue for each worker.  When a job is ready
     * for service we assign its tasks to the workers' queues.
     * 
     * @param job
     */
    public void ServiceJob(FJJob job, int jobPosition, double time) {
        if (job == null) return;
        if (FJSimulator.DEBUG) System.out.println("begin service on job: "+job.path_log_id+"    position: "+jobPosition+"     "+time);
        
        int worker_base = jobWorkers.get(jobPosition).get(0);
        int nworkers = jobWorkers.get(jobPosition).size();
        int worker_index = 0;
        FJTask t = null;
        while ((t = job.nextTask()) != null) {
            workers[0][worker_base + worker_index].queue.add(t);
            worker_index = (worker_index + 1) % nworkers;
        }
        
        // this just added the tasks to the queues.  Check if any
        // workers were idle, and put them to work.
        feedWorkers(time);
    }

    
    /**
     * Enqueue a new job.
     * 
     * This server type has a single job queue where jobs wait until the previous job
     * completes and departs.
     * 
     * @param job
     * @param sample
     */
    public void enqueJob(FJJob job, boolean sample) {
        if (FJSimulator.DEBUG) System.out.println("enqueJob("+job.path_log_id+") "+job.arrival_time);
        
        // only keep a reference to the job if the simulator tells us to
        job.setSample(sample);
        
        job_queue.add(job);
        feedWorkers(job.arrival_time);
        if (FJSimulator.DEBUG) System.out.println("  queued a job.    New queue length: "+job_queue.size());
    }
    
    
    /**
     * Handle a task completion event.  Remove the task from its worker, and
     * give the worker a new task, if available.
     * 
     * @param workerId
     * @param time
     */
    public void taskCompleted(FJWorker worker, double time) {
        if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.path_log_id+" completed "+time);
        FJTask task = worker.current_task;
        task.completion_time = time;
        task.completed = true;
        worker.current_task = null;
        
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
            if (FJSimulator.DEBUG) System.out.println("job departing: "+task.job.path_log_id);
            jobDepart(task.job);
            
            // service the next job, if any
            feedWorkers(time);
        } else {
            serviceTask(worker, worker.queue.poll(), time);
        }
    }


    /**
     * In the multi-queue server we take the queue length to be the rounded average
     * length of all the worker queues.
     */
    @Override
    public int queueLength() {
        return this.job_queue.size();
    }

    @Override
    int numJobsInQueue() {
        return queueLength();
    }
}
