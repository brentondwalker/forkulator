package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class FJTakePatienceBarrierServer extends FJServer {

    /**
     * This is a type of barrier server.  Tasks must start together, and completed tasks optionally
     * block the worker that executed them until the job departs.
     * 
     * Whereas the Take-half schedulers have *Restraint* in that they limit how much of the free
     * resources they claim when starting a new job, this scheduler has *Patience*.  When a job
     * arrives or pops from the queue, the scheduler may delay starting it in the hopes that more
     * resources become available.
     * 
     * The decision has to be made by comparing the expected time we will wait for the next
     * worker to become idle (l --> l+1), and the time that we expect to save by executing with
     * the increased parallelism.  In both cases we cannot compute the expectation exactly because
     * they are order statistics of Erlang RVs.  So in both cases we will approximate with the
     * expectation of a single Erlang.
     */
    
    //XXX- worth factoring out into a FJBarrierServer or FJTakeHalfBarrierServer because so much of this could be inherited...
    
    public Queue<FJJob> job_queue = new LinkedList<FJJob>();

    /**
     * BarrierServers are special because there is a barrier at the start of the
     * jobs.  All tasks must start at the same time, and they reserve the workers
     * until they can start.  This flag adds a barrier at the end as well.
     * Workers remain reserved until all the tasks complete and the job departs.
     */
    private boolean departure_barrier = false;
    
    /**
     * There are (at least) two ways we can approximate the expected time we will need to
     * wait for the next available worker.  Based on the minimum task height, and based on
     * the mean task height.
     */
    public enum TaskWaitComputationType {
        Min, Mean
    }
    public TaskWaitComputationType task_wait_comp_type = TaskWaitComputationType.Min;
    
    /**
     * To study the distributions of job parallelism and other stuff.
     * Easy but sloppy way is just print it out and analyze in matlab.
     * This is very specific to this scheduler type, so I'm just doing
     * this the lazy way.
     */
    public static boolean PRINT_EXTRA_DATA = false;
    
    /*
     * Data structures to keep track of the correspondence between workers and jobs.
     */
    private int remaining_workers;
    private Vector<FJJob> activeJobs;
    private HashMap<FJJob,Vector<Integer>> job2workers;
    private HashMap<FJJob,Integer> job2parallelism;
    private FJJob[] worker2job;
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJTakePatienceBarrierServer(int num_workers, boolean departure_barrier, TaskWaitComputationType task_wait_comp_type) {
        super(num_workers);
        this.departure_barrier = departure_barrier;
        this.task_wait_comp_type = task_wait_comp_type;
        System.err.println("FJTakePatienceBarrierServer(departure_barrier="+departure_barrier+", "+task_wait_comp_type+")");
        
        for (int i=0; i<num_workers; i++) {
            workers[0][i].queue = new LinkedList<FJTask>();
        }
        
        remaining_workers = num_workers;

        activeJobs = new Vector<FJJob>();
        job2workers = new HashMap<FJJob,Vector<Integer>>();
        job2parallelism = new HashMap<FJJob,Integer>();
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
    @Override
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
                    // how many workers did this job originally get?
                    // we can't use job2workers because workers are removed from that as they finish.
                    // add a new hashmap to keep track of it.
                    if (PRINT_EXTRA_DATA) System.out.println("THTC\t"+time+"\t"+job.arrival_time+"\t"+remaining_workers+"\t"+job2parallelism.get(job));
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
            job2parallelism.remove(job);
        }
        
        if (remaining_workers > 0) {
          ServiceJob(this.job_queue.poll(), time);
        }
    }

    /**
     * Approximate the reduction in execution time we can expect from getting another worker.
     * 
     * We don't expect this to be called in the case of no idle workers, but in case it is, 
     * it will return infinity.
     * 
     * @param job
     * @return
     */
    private double estimate1WorkerSpeedup(FJJob job) {
        double k = job.num_tasks;
        int l = 0;
        boolean all_idle = true;
        for (FJWorker w : workers[0]) {
            if (w.current_task == null) {
                l++;
                //System.out.println("\t +++ increment l to l="+l);
            } else {
                all_idle = false;
                //System.out.println("\t +++ set all_idle="+all_idle);
            }
        }
        //System.out.println("\t num idle: l="+l);
        // failsafe against bugs elsewhere
        if (all_idle) {
            //System.out.println("\t all workers idle!");
            return 0;
        }        
        return (l==0) ? Double.POSITIVE_INFINITY : (k/((double)l) - k/((double)l + 1));
    }
    
    /**
     * Approximate the time we expect to wait for the next worker to become available.
     * This will be based on the current state of the workers, and the estimation method chosen.
     * 
     * @return
     */
    private double estimate1WorkerWait() {
        double worker_wait = Double.POSITIVE_INFINITY;
        switch (task_wait_comp_type) {
        case Mean:
            int H = 0;
            int b = 0;
            for (FJWorker w : workers[0]) {
                //XXX warning: this assumes the current task is executing, and not just blocking.
                if (w.current_task != null) {
                    b++;
                    H += w.queue.size() + 1;
                }
            }
            worker_wait = ((double)H)/((double)b);
            break;
            
        case Min:
            int H1 = Integer.MAX_VALUE;
            boolean all_idle = true;
            for (FJWorker w : workers[0]) {
                if (w.current_task != null) {
                    if (w.current_task != null) {
                        H1 = (w.queue.size()+1 < H1) ? w.queue.size()+1 : H1;
                        //System.out.println("\t +++ set H1="+H1+"\t queue_size="+w.queue.size());
                        all_idle = false;
                    }
                }
            }
            worker_wait = (all_idle) ? 0.0 : (double)H1;
            break;
        
        default:
            break;
        }
        
        return worker_wait;
    }
    
    /**
     * Enqueue a new job.
     * 
     * This type of server has a separate queue for each worker.  When a job is ready
     * for service we assign its tasks to the workers' queues.
     * 
     * This method is key to the Patience Barrier Server.  Here we either start servicing the job,
     * or we defer until later.
     * 
     * @param job
     */
    public void ServiceJob(FJJob job, double time) {
        if (job == null) return;
        if (remaining_workers == 0) return;
        if (FJSimulator.DEBUG) System.out.println("ServiceJob: "+job.path_log_id+"     "+time);
        
        // decide whether to start the job or not...
        //System.out.println("compare: "+estimate1WorkerSpeedup(job)+" \t vs \t"+estimate1WorkerWait()+"\t(remaining_workers="+this.remaining_workers+")");
        if (estimate1WorkerSpeedup(job) > estimate1WorkerWait()) {
            //if (FJSimulator.DEBUG) 
            //System.out.println("\t... defer job start (remaining_workers="+this.remaining_workers+")");
            return;
        }

        // pick out some number of workers to use
        int nworkers = (remaining_workers == 1) ? 1 : (int)Math.max(1, Math.min(remaining_workers - 1, remaining_workers));
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
        job2parallelism.put(job,  worker_pool.size());
        
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
    @Override
    public void enqueJob(FJJob job, boolean sample) {
        if (FJSimulator.DEBUG) System.out.println("enqueJob("+job.path_log_id+") "+job.arrival_time);
        
        // only keep a reference to the job if the simulator tells us to
        job.setSample(sample);

        // analysis of the algorithm
        // PROBLEM: this records the remaining workers at the moment after the last job started
        if (PRINT_EXTRA_DATA) System.out.println("THBS\t"+job.arrival_time+"\t"+job.arrival_time+"\t"+remaining_workers+"\t"+remaining_workers);

        job_queue.add(job);
        feedWorkers(job.arrival_time);
        if (FJSimulator.DEBUG) System.out.println("  queued a job.    New queue length: "+job_queue.size());
    }

    @Override
    public void taskCompleted(FJWorker worker, double time) {
        if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.path_log_id+" completed "+time);
        FJTask task = worker.current_task;
        task.completion_time = time;
        task.completed = true;
        
        // if there is a departure barrier, just leave the completed task
        // on the worker and clear them out when the job is done.
        // this does not work!!  
        // Even with a departure barrier, the worker has to service all the sub-tasks in its queue.
        // This is handled in feedWorkers().
        //if (! departure_barrier) {
            worker.current_task = null;
        //}
        
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
