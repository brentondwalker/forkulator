package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;


public class FJTakeHalfSplitMergeServer extends FJServer {

    /**
     * This server is like SplitMerge, except when a job gets serviced, it
     * only uses half of the available servers.  The tasks are assigned to workers
     * when the job starts, so within a job, it's not like single queue.
     * 
     * This is different from "Halfway" split merge because an incoming job only
     * takes half of the servers available at that moment (or 1).  In the Halfway
     * server, there are fixed "job positions" with fixed numbers of servers, so 
     * the server really acts like a collection of parallel SM servers.  This
     * server is in practice, more conservative in how many workers each job gets.
     * It will always be less than or equal to the number it would have gotten
     * in the Halfway server.
     * 
     * Under load, I expect that this server will degenerate to a cluster of
     * independent parallel servers.  The size of the worker pools will keep
     * getting smaller until it's just a collection of singletons.
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
     * Data structures to keep track of the correspondence between workers and jobs.
     */
    private int remaining_workers;
    private Vector<FJJob> activeJobs;
    private HashMap<FJJob,Vector<Integer>> job2workers;
    private FJJob[] worker2job;

    /*
     * Generalize this scheduler to fractions other than 1/2.
     * Still keep the rule that a job won't take the last worker unless
     * there's only one worker left.
     */
    private double take_fraction = 0.5;
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJTakeHalfSplitMergeServer(int num_workers, double take_fraction) {
        super(num_workers);
        System.err.println("FJTakeHalfSplitMergeServer("+take_fraction+")");
        
        this.take_fraction = take_fraction;
        for (int i=0; i<num_workers; i++) {
            workers[0][i].queue = new LinkedList<FJTask>();
        }
        
        remaining_workers = num_workers;

        activeJobs = new Vector<FJJob>();
        job2workers = new HashMap<FJJob,Vector<Integer>>();
        worker2job = new FJJob[num_workers];
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
        // in this server type we iterate over the active jobs' worker pools
        Vector<FJJob> completed_jobs = new Vector<FJJob>();
        for (FJJob job : activeJobs) {
            // and within each worker pool, iterate over the workers
            boolean all_idle = true;
            for (int w : job2workers.get(job)) {
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
            // job.  Put these workers back into circulation.
            if (all_idle) {
                for (int w : job2workers.get(job)) {
                    worker2job[w] = null;
                    //System.out.println("freeing worker "+w);
                    remaining_workers++;
                }
                completed_jobs.add(job);
            }
        }

        for (FJJob job : completed_jobs) {
            activeJobs.remove(job);
            job2workers.remove(job);
        }
        
        if (remaining_workers > 0) {
          ServiceJob(this.job_queue.poll(), time);
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
    public void ServiceJob(FJJob job, double time) {
        if (job == null) return;
        if (remaining_workers == 0) return;
        if (FJSimulator.DEBUG) System.out.println("begin service on job: "+job.path_log_id+"     "+time);

        // pick out some number of workers to use
        int nworkers = (remaining_workers == 1) ? 1 : (int)Math.max(1, Math.min(remaining_workers - 1, remaining_workers * take_fraction));
        
        activeJobs.add(job);
        Vector<Integer> worker_pool = new Vector<Integer>();
        job2workers.put(job, worker_pool);
        for (int w=0; (w<num_workers && worker_pool.size()<nworkers); w++) {
            if (worker2job[w] == null) {
                worker_pool.add(w);
                worker2job[w] = job;
                remaining_workers--;
            }
        }
        
        //System.out.println("worker_pool.size() = "+worker_pool.size()+"   nworkers = "+nworkers+"   activeJobs.size() = "+activeJobs.size()+"   queue size = "+this.job_queue.size());
        
        // assign the job's tasks to the workers
        int worker_index = 0;
        FJTask t = null;
        while ((t = job.nextTask()) != null) {
            workers[0][worker_pool.get(worker_index)].queue.add(t);
            //System.out.println("assigning task "+t+" to worker "+worker_pool.get(worker_index));
            worker_index = (worker_index + 1) % nworkers;
        }
        
        // These newly allocated workers will be idle.  Put them to work.
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
    
    
    
}
