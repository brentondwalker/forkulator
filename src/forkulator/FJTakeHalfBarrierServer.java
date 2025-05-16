package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class FJTakeHalfBarrierServer extends FJServer {

	/**
	 * This is a combination of FJTakeHalfSplitMergeServer and FJBarrierServer.  
	 * 
	 * Does this make any sense?
	 * - in the case of a start barrier, doing TH just effectively removes the
	 *   barrier. When a job arrives, if there are any idle servers, it always
	 *   gets serviced.  Its just a question of how much parallelism.  What
	 *   about departure?  In the case of original THSM, the job blocked its
	 *   workers until the job departed.  With start-barrier only, the workers
	 *   will become available sooner.
	 *   So the differences are that we allow k<s, and workers become available
	 *   immediately after completing their task.
	 * - in the 2-barrier case, the only difference is that we allow k<s.
	 * 
	 * How much of a difference is allowing k<s?  For one thing, a job could
	 * be serviced with full parallelism if s>=2k, unlike original THSM.
	 * Generally it's just a change to the properties of the composite tasks
	 * the workers have to process.  If k<s, the composite tasks will be sums
	 * of strictly fewer things.
	 * 
	 * *BUT* the old THSM implementation actually did not require k=s.  That is
	 * just the way I always ran it.  Actually no change is required there.
	 * 
	 * 
	 */
	
    public Queue<FJJob> job_queue = new LinkedList<FJJob>();
    
    /**
     * BarrierServer is special because there is a barrier at the start of the
     * jobs.  All tasks must start at the same time, and they reserve the workers
     * until they can start.  This flag adds a barrier at the end as well.
     * Workers remain reserved until all the tasks complete and the job departs.
     */
    private boolean departure_barrier = false;
        
    /**
     * The fraction of available servers to take for the next available job.
     */
    private double take_fraction = 1.0;
    
    /**
     * This type of server has a separate queue for each worker.
     */
    
    /**
     * To study the distributions of job parallelism and other stuff.
     * Easy but sloppy way is just print it out and analyze in matlab.
     * This is very specific to this scheduler type, so I'm just doing
     * this the lazy way.
     */
    public static boolean PRINT_EXTRA_DATA = true;
    
    /*
     * Data structures to keep track of the correspondence between workers and jobs.
     */
    private int remaining_workers;
    private Vector<FJJob> activeJobs;
    private HashMap<FJJob,Vector<Integer>> job2workers;
    private FJJob[] worker2job;
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJTakeHalfBarrierServer(int num_workers, boolean departure_barrier, double take_fraction) {
        super(num_workers);
        this.departure_barrier = departure_barrier;
        this.take_fraction = take_fraction;
        System.err.println("FJTakeHalfBarrierServer(departure_barrier="+departure_barrier+", "+take_fraction+")");
        
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
     * In a barrier server we only service the entire job at once.
     * We never have a partially serviced job, and we don't have to
     * queue tasks at the workers.
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
            Vector<Integer> freed_workers = new Vector<Integer>();
            for (int w : job2workers.get(job)) {
                if (workers[0][w].current_task == null) {
                    // if the worker is idle, pull the next task (or null) from its queue
                    serviceTask(workers[0][w], workers[0][w].queue.poll(), time);
                }
                
                // check if a task is being serviced now
                if (workers[0][w].current_task != null) {
                    all_idle = false;
                } else if (this.departure_barrier == false) {
                	// if there is no departure barrier, then put the idle workers
                	// back into circulation immediately.
                	worker2job[w] = null;
                	//System.out.println("freeing worker "+w+" (no barrier)");
                	freed_workers.add(w);
                	remaining_workers++;
                }
            }
            job2workers.get(job).removeAll(freed_workers);
            
            // if all the workers are idle it means we have completed the current
            // job.  Put these workers back into circulation.
            if (all_idle) {
            	if (this.departure_barrier) {
	                for (int w : job2workers.get(job)) {
	                    worker2job[w] = null;
	                    //System.out.println("freeing worker "+w+" (departure barrier)");
	                    remaining_workers++;
	                }
	                //System.out.println("");
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
        //int initially_remaining_workers = remaining_workers;
        if (PRINT_EXTRA_DATA) System.out.println("THJS\t"+job.arrival_time+"\t"+time+"\t"+nworkers+"\t"+remaining_workers);
        
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
        
        //System.out.println("remaining_workers = "+initially_remaining_workers+"   worker_pool.size() = "+worker_pool.size()+"   nworkers = "+nworkers+"   activeJobs.size() = "+activeJobs.size()+"   queue size = "+this.job_queue.size());
        
        // assign the job's tasks to the workers
        int worker_index = 0;
        FJTask t = null;
        while ((t = job.nextTask()) != null) {
            workers[0][worker_pool.get(worker_index)].queue.add(t);
            //System.out.println("assigning job "+job+" task "+t+" to worker "+worker_pool.get(worker_index));
            worker_index = (worker_index + 1) % nworkers;
        }
        
        // These newly allocated workers will be idle.  Put them to work.
        feedWorkers(time);
    }
	
     
     /**
      * Enqueue a new job.
      * 
      * This server type has a single job queue where jobs wait until enough workers
      * are available to start all the tasks at once.
      * 
      * @param job
      * @param sample
      */
     public void enqueJob(FJJob job, boolean sample) {
    	 if (FJSimulator.DEBUG) System.out.println("enqueJob("+job.path_log_id+") "+job.arrival_time);
         
         // only keep a reference to the job if the simulator tells us to
         job.setSample(sample);

         // analysis of the algorithm
         // PROBLEM: this records the remaining workers at the moment after the last job started
         if (PRINT_EXTRA_DATA) System.out.println("THBS\t"+job.arrival_time+"\t"+job.arrival_time+"\t"+remaining_workers+"\t"+remaining_workers);
         int rwk = 0;
         

         job_queue.add(job);
         feedWorkers(job.arrival_time);
         if (FJSimulator.DEBUG) System.out.println("  queued a job.    New queue length: "+job_queue.size());
     }


     /**
      * Handle a task completion event.  Remove the task from its worker, and
      * check if we can service the next job.
      * 
      * @param workerId
      * @param time
      */
     public void taskCompleted(FJWorker worker, double time) {
         if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.path_log_id+" completed "+time);
         FJTask task = worker.current_task;
         task.completion_time = time;
         task.completed = true;
         
         // if there is a departure barrier, just leave the completed task
         // on the worker and clear them out when the job is done.
         if (! departure_barrier) {
        	 worker.current_task = null;
         }
         
         // check if this task was the last one of a job
         //TODO: this could be more efficient
         boolean compl = true;
         for (FJTask t : task.job.tasks) {
             compl = compl && t.completed;
         }
         task.job.completed = compl;
         
         // XXX is this correct?
         // 1. if the job is completed, we feed the workers
         // 2. else try to service the next task in this worker's queue
         // 3. but what if this worker's queue is empty, there is no departure barrier, and
         //    the job is not done??
         if (task.job.completed) {
        	 // if there is a departure barrier, clear out the tasks
        	 if (departure_barrier) {
        		 for (FJTask t : task.job.tasks) {
        			 t.worker.current_task = null;
        		 }
        	 }
        	 
             // it is the last, record the completion time
             task.job.completion_time = time;
             
             // for this type of server it is also the departure time
             task.job.departure_time = time;
             
             // sample and dispose of the job
             if (FJSimulator.DEBUG) System.out.println("job departing: "+task.job.path_log_id);
             jobDepart(task.job);
             
             // service the next job, if possible, if any
             feedWorkers(time);
         } else {
             //XXX There was a possible bug.  With *no* departure barrier, when a worker completed all its
             //    tasks, the worker would not be put back into service until the next job got queued, or
             //    the job departed.  It was like a partial departure barrier.
             //  This was the error: serviceTask(worker, worker.queue.poll(), time);
             // Calling feedWorkers takes care of everything here...
             feedWorkers(time);
         }
     }
     
     
	@Override
	public int queueLength() {
        return this.job_queue.size();
	}

}
