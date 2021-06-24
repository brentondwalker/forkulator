package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;


public class FJTakeHalfSplitMergeBackpressureServer extends FJServer {

    /**
     * This is like FJTakeHalfSplitMergeServer, but adds the variation that
     * the number of workers a job is allowed, also depends on the state of the
     * job queue.  For example, if a job arrives to a completely empty system,
     * why should it not take all the workers?
     * 
     * The simplest version of this idea is that if the job queue is empty, then
     * the next job can take all the available workers.
     * 
     * Another idea is that if the queue is empty, maybe the next job should wait
     * a bit for more servers to become available before it begins executing.
     * Some weighted average of the past job queue length.
     * 
     * Say a job is at the head of the job queue.  If there are no jobs waiting
     * behind it, then (if all workers are not already free) it waits for G more
     * workers to become free.
     * 
     * What you can expect here is diminishing returns.  If we start with just
     * one worker available, waiting for a second has a huge payoff, and the
     * expected wait time is the min() of a bunch of iid RVs (num_servers - 1).
     * But if all but one worker is free, the benefit of waiting is negligible,
     * and the cost of waiting is maximal, because it is the min() of one thing.
     * 
     * Consider expected benefit of waiting vs expected cost.
     * 
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
     * the patience flag determines whether or not a job can wait for more
     * workers to become available, or if it always starts immediately on the
     * first worker available.
     */
    private boolean patience = false;
    private FJJob patient_job = null;
    private int idle_workers_needed = 0;
    private static final int JOB_QUEUE_PATIENCE_THRESHOLD = 0;
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJTakeHalfSplitMergeBackpressureServer(int num_workers, boolean patience_flag) {
        super(num_workers);
        System.out.println("FJTakeHalfSplitMergeServer()");
        
        this.patience = patience_flag;
        
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
        	if (this.patient_job != null) {
        		ServiceJob(this.patient_job, time);
        	} else {
        		ServiceJob(this.job_queue.poll(), time);
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
    public void ServiceJob(FJJob job, double time) {
        if (job == null) return;
        if (remaining_workers == 0) return;
        if (FJSimulator.DEBUG) System.out.println("begin service on job: "+job.path_log_id+"     "+time);

        // if the patience flag is set, then check the job queue to see if
        // this job can wait for more workers to become idle
        /*
        if (this.patient_job==null && patience) {
        	if (this.job_queue.size() <= JOB_QUEUE_PATIENCE_THRESHOLD) {
        		this.patient_job = job;
        		// compute how many workers we want to have idle before we start
        		//this.idle_workers_needed = Math.min(job.num_tasks, Math.min(remaining_workers+1, num_workers));
        		/**
        		 * Just waiting for one more worker, always, is kind-of dumb.  If there is only
        		 * one worker available, waiting for a 2nd doubles the parallelism of the next job.
        		 * Also, if a lot of jobs are in service, the expected waiting time until the
        		 * next departure is shorter.  OTOH, if almost all workers are already idle, then
        		 * the benefit of one more worker is negligible, and less jobs in progress means a
        		 * longer expected wait until another becomes available. So when deciding whether
        		 * to be patient or not, we should consider:
        		 * - the number of currently idle workers
        		 * - the fraction of currently idle workers
        		 * - the number of jobs in progress (and how many workers they have)
        		 * - 
        		 * /
        		int num_jobs_in_progress = this.activeJobs.size();
        		double idle_fraction = (1.0*remaining_workers)/(1.0*num_workers);
        		int busy_workers = num_workers - remaining_workers;
        		double workers_per_job = (1.0*busy_workers)/(1.0*num_jobs_in_progress);
        		
        		// the expected factor to be gained by a job departing
        		double rr = workers_per_job / remaining_workers;
        		
        		if ((rr >= 1.5) &&  (num_jobs_in_progress >= num_workers/4.0)) {
        			this.idle_workers_needed = Math.min(job.num_tasks, Math.min(remaining_workers+1, num_workers));
        			System.out.println("** Setting a job to be patient rr="+rr);
        		} else {
        			System.out.println("-- J="+num_jobs_in_progress+"\t I="+remaining_workers+"\t B="+busy_workers+"\t rr="+rr);
        			this.idle_workers_needed = remaining_workers;
        		}
        	}
        }
        */

        /*
         * Try a really restrictive version of BackPressure and Patience.
         * Jobs are only patient in the case where there is only one available
         * worker, and at least k/2 jobs in progress.  Also the only time the job
         * takes all workers is the case of a patient job.
         */
        if (this.patient_job==null && patience) {
        	if ((this.job_queue.size() <= JOB_QUEUE_PATIENCE_THRESHOLD) 
        			&& (remaining_workers==1)
        			&& (activeJobs.size() >= num_workers/2)) {
        		this.patient_job = job;
    			this.idle_workers_needed = remaining_workers + 1;
        	}
        }

        
        // if we are going to wait for more workers, just return
        //TODO: if the job queue is no longer empty, should just start the job?
        boolean servicing_patient_job = false;
        if (patience && this.patient_job!=null) {
        	if (this.patient_job != job) {
        		System.err.println("ERROR: trying to service a job when another is being patient!");
        		System.exit(1);
        	}
        	
        	if (remaining_workers < idle_workers_needed) {
        		//System.out.println("Being patient for job remaining_workers="+remaining_workers+"  idle_workers_needed="+idle_workers_needed);
        		return;
        	}
        	
        	// we're going to service the job that's been waiting
        	this.patient_job = null;
        	idle_workers_needed = 0;
        	servicing_patient_job = true;
        	//System.out.println("Servicing a patient job   "+remaining_workers);
        }
        
        // pick out some number of workers to use
        // if the job queue is below some threshold, take all the workers
        int nworkers = (remaining_workers == 1) ? 1 : remaining_workers/2;
        //if (this.job_queue.isEmpty()) {
        if (this.job_queue.isEmpty() && servicing_patient_job) {
        	//System.out.println("  ** giving a patient job all the workers: "+remaining_workers);
        	nworkers = remaining_workers;
        }
        
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
