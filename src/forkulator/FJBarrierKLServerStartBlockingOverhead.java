package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

import forkulator.randomprocess.ConstantIntertimeProcess;
import forkulator.randomprocess.UniformIntertimeProcess;
import forkulator.randomprocess.TriangleIntertimeProcess;
import forkulator.randomprocess.LayerCakeIntertimeProcess;
import forkulator.randomprocess.IntertimeProcess;

public class FJBarrierKLServerStartBlockingOverhead extends FJServer {

    /**
     * This will be like FJBarrierServerOverhead, with the non-blocking pre-departure overhead,
     * but with uniformly random blocking pre-start overhead meant to model the behavior of
     * Spark BEM systems with no departure barrier.
     * In those cases there is a substantial delay between when the cluster becomes available to
     * service the next job, and when the new job starts.  But that overhead does not appear in
     * the sojourn time of either job.  It does affect the waiting time, however.
     * 
     */
    /**
     * This variation on the server type is the K-L version.   
     * That means that it contains K tasks, but is free to depart when L of K tasks complete,
     * and it ends the unfinished tasks when it departs.
     */
    /*XXX
     * Note that in the old (k,l) servers, abandoned tasks were left to keep running, even when the job departed.
     * in this version, to model the process described in "DD-PPO: L EARNING N EAR -P ERFECT P OINT G OAL
     * NAVIGATORS FROM 2.5 B ILLION F RAMEs", we kill the stragglers.
     */
    
    /**
     * The (k,l) server considers a job done when l of its k tasks complete.
     * The remaining tasks are still left to run to completion.
     * 
     */
    public int l;
    
    
    public double job_predeparture_overhead = 0.0;

    public double task_predeparture_overhead = 0.0;
    
    public double blocking_start_overhead_lower = 0.0;
    public double blocking_start_overhead_upper = 0.0;

    public IntertimeProcess pre_departure_overhead_process = null;
    
    public IntertimeProcess start_blocking_overhead_process = null;

    /**
     * To implement pre-departure overhead, once all the tasks finish and
     * the job is marked "complete", we queue a dummy task on a dummy worker
     * that has a fixed execution time equal to the desired pre-departure
     * overhead.  When this task completes, then the job can depart.
     */
    //XXX it is not enough to have a single dummy overhead task/worker for the server.
    // In this server type multiple jobs can be active at once.
    /**
     * This is an important point for how we model the pre-departure overhead.  Does the
     * pre-departure overhead of different jobs get serviced in parallel (unlimited parallelism),
     * or does it have to wait in a queue?  The idea of the PDO is that it is overhead related
     * to gathering results and cleaning up in the driver, and the driver does not have unlimited
     * capacity, so I am implementing this as a queue.  In general the PDO is very small, at
     * least for the no-data jobs we experiment with, so very likely this will not make any
     * difference.
     * 
     * I do not yet have an experimental result to decide if this is correct or not.
     * 
     * Entirely possible the PDO should be proportional to the total processing time of all tasks
     * in the job, vs just the number of tasks.  Our view of this is influenced by the fact that
     * our tasks have trivial data.  Compare to the forkulator simulation jobs, where each task
     * produces a huge ammt of data.  in that case the final step of collecting results is often
     * more than the actual processing time.  But that would not be considered overhead in Spark.
     * It would mostly be accounted for in the shuffle and merge stages.
     */
    public Queue<FJTask> dummy_overhead_task_queue = new LinkedList<FJTask>();
    public FJWorker dummy_overhead_worker = null;

    /**
     * For each worker that is blocked by start overhead, keep track of the task that is blocking it.
     */
    public HashMap<FJWorker, FJTask> blocked_workers = new HashMap<FJWorker, FJTask>();
    
    /**
     * Keep track of which workers are blocked by a dummy start overhead task
     */
    public HashMap<FJTask, Vector<FJWorker>> dummy_task_2_blocked_workers = new HashMap<FJTask, Vector<FJWorker>>();
    
    /**
     * Keep track of jobs that are currently start-blocked, and the dummy task they are waiting for.
     */
    public HashMap<FJJob, FJTask> blocked_jobs = new HashMap<FJJob, FJTask>();
    
    /**
     * This server is to simulate jobs running on barrier RDDs in spark.
     * The constraint on these jobs is that all tasks must start at the same time.
     * So if there are t tasks in the job, no tasks can be serviced until at least
     * t workers are free.  If t=w then anticipate that this will act like
     * Split-Merge.
     * 
     * Design issue: should this server assume that all jobs have the same number
     * of tasks?  If we assume that, then we never need to look deeper than the
     * head of the queue.  If not, then there is the question of whether, when
     * there are m workers idle, should we go into the job queue to find a job
     * with m or fewer tasks?
     * At least for now, we won't do this.  Only process jobs in FIFO order.
     * Jobs cannot overtake each other before they begin servicing.
     */
    
    public Queue<FJJob> job_queue = new LinkedList<FJJob>();

    //FJJob next_job = null;
    
    /**
     * BarrierServer is special because there is a barrier at the start of the
     * jobs.  All tasks must start at the same time, and they reserve the workers
     * until they can start.  This flag adds a barrier at the end as well.
     * Workers remain reserved until all the tasks complete and the job departs.
     */
    private boolean departure_barrier = false;
    
    /**
     * But just for the sake of generality, let's make the start barrier optional too.
     * This way we can experiment with jobs that have the departure barrier, but not
     * the BEM-style start barrier.
     * If you turn off both start and departure barriers, this server type should
     * behave like single-queue fork join.
     */
    //private boolean start_barrier = true;
    
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJBarrierKLServerStartBlockingOverhead(int num_workers,
            int l,
            boolean start_barrier,
            boolean departure_barrier,
            double job_predeparture_overhead,
            double task_predeparture_overhead,
            double blocking_start_overhead_lower,
            double blocking_start_overhead_upper) {
        super(num_workers);
        this.l = l;
        this.departure_barrier = departure_barrier;
        //this.start_barrier = start_barrier;
        if (!start_barrier) {
            System.err.println("ERROR: FJBarrierServerStartBlockingOverhead only works when you have a start barrier.");
            System.exit(0);
        }        
        this.job_predeparture_overhead = job_predeparture_overhead;
        this.task_predeparture_overhead = task_predeparture_overhead;
        this.blocking_start_overhead_lower = blocking_start_overhead_lower;
        this.blocking_start_overhead_upper = blocking_start_overhead_upper;
        //this.start_blocking_overhead_process = new UniformIntertimeProcess(blocking_start_overhead_lower, blocking_start_overhead_upper);
        this.start_blocking_overhead_process = new LayerCakeIntertimeProcess(new UniformIntertimeProcess(blocking_start_overhead_lower, blocking_start_overhead_upper), 1.0,
                new TriangleIntertimeProcess(blocking_start_overhead_lower, blocking_start_overhead_upper, blocking_start_overhead_lower), 0.5);
        this.dummy_overhead_worker = new FJWorker();
        System.err.println("FJBarrierServerOverhead(departure_barrier="+departure_barrier+" , start_barrier="+start_barrier+")"
                +", job_predeparture_overhead="+job_predeparture_overhead+", task_predeparture_overhead="+task_predeparture_overhead
                +", blocking_start_overhead_lower="+blocking_start_overhead_lower+", blocking_start_overhead_upper="+blocking_start_overhead_upper+")");
    }
    
    /**
     * Simple Constructor
     * 
     * This version is for when you don't want the extra overhead.
     * Omit those parameters, and this will allocate constant IntertimeProcesses with value of zero.
     * 
     * @param num_workers
     */
    public FJBarrierKLServerStartBlockingOverhead(int num_workers, int l, boolean start_barrier, boolean departure_barrier) {
        super(num_workers);
        this.l = l;
        this.departure_barrier = departure_barrier;
        //this.start_barrier = start_barrier;
        if (!start_barrier) {
            System.err.println("ERROR: FJBarrierServerStartBlockingOverhead only works when you have a start barrier.");
            System.exit(0);
        }
        this.job_predeparture_overhead = 0.0;
        this.task_predeparture_overhead = 0.0;
        this.blocking_start_overhead_lower = 0.0;
        this.blocking_start_overhead_upper = 0.0;
        this.start_blocking_overhead_process = new ConstantIntertimeProcess(0.0, true);
        this.dummy_overhead_worker = new FJWorker();
        System.err.println("FJBarrierServerOverhead(departure_barrier="+departure_barrier+" , start_barrier="+start_barrier+")"
                +", job_predeparture_overhead="+job_predeparture_overhead+", task_predeparture_overhead="+task_predeparture_overhead
                +", blocking_start_overhead_lower="+blocking_start_overhead_lower+", blocking_start_overhead_upper="+blocking_start_overhead_upper+")");
    }
    
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJBarrierKLServerStartBlockingOverhead(int num_workers, int l, double job_predeparture_overhead, double task_predeparture_overhead, double blocking_start_overhead_lower, double blocking_start_overhead_upper) {
        super(num_workers);
        this.l = l;
        this.job_predeparture_overhead = job_predeparture_overhead;
        this.task_predeparture_overhead = task_predeparture_overhead;
        this.blocking_start_overhead_lower = blocking_start_overhead_lower;
        this.blocking_start_overhead_upper = blocking_start_overhead_upper;
        this.dummy_overhead_worker = new FJWorker();
        System.err.println("FJBarrierServerOverhead("+"job_predeparture_overhead="+job_predeparture_overhead+", task_predeparture_overhead="+task_predeparture_overhead
                +", blocking_start_overhead_lower="+blocking_start_overhead_lower+", blocking_start_overhead_upper="+blocking_start_overhead_upper+")");
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
        if (FJSimulator.DEBUG) System.out.println("feedWorkers("+time+")");
         if (job_queue.size() == 0) {
             return;
         }
         
         // count the number of idle workers
         Vector<Integer> idle_workers = new Vector<Integer>();
         for (int w=0; w<num_workers; w++) {
             if (workers[0][w].current_task == null) {
                 if (!blocked_workers.containsKey(workers[0][w])) { // filter out start-blocked workers
                     idle_workers.add(w);
                 }
             }
         }
         
         // can we service this job?
         // XXX This only works with start_barrier=true
         if ((job_queue.peek() != null) && (idle_workers.size() >= job_queue.peek().num_tasks))  {
             // In this case, with blocking start overhead, we can't simply start the next batch of tasks
             // when the workers become available.  We have to schedule the blocking start overhead, and then the
             // job actually starts.
             // In the meantime we have to block the workers so we don't keep popping jobs out of the queue each
             // time a task finishes...  This is getting messy.
             // We can't have just a single start-blocker task for the server either.  That will break things in
             // cases where k is small.  We need to actually put something on the workers, and then link it back
             // to this job that is waiting to start.
             
             // we can't service the job immediately.  There is the blocking start overhead.
             // first create the dummy overhead task
             FJJob job = job_queue.poll();
             if (job != null) {
                 FJTask start_blocker_task = new FJTask(start_blocking_overhead_process, time, job);
                 blocked_jobs.put(job, start_blocker_task);
                 
                 int w = 0;
                 Vector<FJWorker> newly_blocked_workers = new Vector<FJWorker> ();
                 while (w<job.num_tasks && w < idle_workers.size()) {
                     FJWorker worker = workers[0][idle_workers.get(w)];
                     blocked_workers.put(worker, start_blocker_task);
                     newly_blocked_workers.add(worker);
                     worker.current_task = start_blocker_task;
                     w++;
                 }
                 dummy_task_2_blocked_workers.put(start_blocker_task, newly_blocked_workers);
                 
                 // just use any one of the blocked workers to service the task
                 serviceTask(newly_blocked_workers.get(0), start_blocker_task, time);
             }

             feedWorkers(time);
         }
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
         
         job_queue.add(job);
         feedWorkers(job.arrival_time);

         if (FJSimulator.DEBUG) System.out.println("  queued a job.    New queue length: "+job_queue.size());
     }


     /**
      * Unlike the old (k,l) server models, we kill the stragglers processes, so they are free to service other jobs.
      */
     private void killStragglers(FJJob job, double time) {
         for (FJTask t : job.tasks) {
             if (! t.completed) {
                 // do we really need to un-schedule its completion?
                 // We just need to ensure that nothing happens when its completion event actually fires.
                 // it seems sloppy, though.
                 t.worker.current_task = null;
                 t.completed = true;
                 t.abandoned = true;
                 t.completion_time = time;
                 
                 // tell the simulator to ignore this tasks's completion event
                 t.completion_event.deleted = true;
             }
         }
     }
     
     /**
      * Handle a task completion event.  Remove the task from its worker, and
      * check if we can service the next job.
      * 
      * XXX: If there is pre-departure overhead, should a departure barrier block before,
      * or after that overhead has passed?
      * 
      * @param workerId
      * @param time
      */
     public void taskCompleted(FJWorker worker, double time) {
         if (FJSimulator.DEBUG) System.out.println("task "+worker.current_task.path_log_id+" completed "+time);
         FJTask task = worker.current_task;
         task.completion_time = time;
         task.completed = true;

         // check if this was a start-barrier overhead dummy task
         if (blocked_workers.containsKey(worker)) {
             // start the actual tasks of the job
             // clean up all the messy structures
             
             // get the job that's about to be serviced
             FJJob job = task.job;
             blocked_jobs.remove(job); //XXX could get rid of blocked_jobs structure.  It's only useful for debugging.
             if (FJSimulator.DEBUG) System.out.println("job unblocked. "+blocked_jobs.size()+" jobs remain blocked at time "+time);
             
             Vector<FJWorker> workers_blocked_by_this_task = dummy_task_2_blocked_workers.get(task);
             for (FJWorker wrk : workers_blocked_by_this_task) {
                 // first clear out the workers
                 wrk.current_task = null;
                 
                 // service the actual tasks
                 FJTask nt = job.nextTask();
                 serviceTask(wrk, nt, time);
                 
                 // clean up the auxillary structures
                 blocked_workers.remove(wrk);
             }
             dummy_task_2_blocked_workers.remove(task);
             workers_blocked_by_this_task.clear();
         }
         // check if this was the end of the pre-departure overhead dummy task
         else if (task.job.completed && worker == dummy_overhead_worker) {
             //System.out.println("Dummy task completed...");
             
             task.completed = true;

             // clear out this dummy worker
             dummy_overhead_worker.current_task = null;
             
             // departure time will be different from completion time
             task.job.departure_time = time;
                          
             // sample and dispose of the job
             if (FJSimulator.DEBUG) System.out.println("job departing: "+task.job.path_log_id+"\t"+time);
             jobDepart(task.job);
             
             // schedule the next pre-departure dummy task, if there is one
             serviceTask(dummy_overhead_worker, dummy_overhead_task_queue.poll(), time);
             
         } else {
             // if there is a departure barrier, just leave the completed task
             // on the worker and clear them out when the job is done.
             // XXX: by putting this here, (inside the else block) we are making pre-departure overhead
             //      block the server when a departure barrier is in place.
             if (! departure_barrier) {
                 worker.current_task = null;
             }

             // check if this task was the last one of a job
             // It is last if it is the l'th task to finish
             //TODO: this could be more efficient
             int num_complete = 0;
             for (FJTask t : task.job.tasks) {
                 num_complete += (t.completed) ? 1 : 0;
             }
             if (num_complete >= this.l) {
                 task.job.completed = true;
             }

             if (task.job.completed) {
                 
                 // since this is a (k,l) server, need to clear out any straggler tasks we abandon.
                 killStragglers(task.job, time);

                 // if there is a departure barrier, clear out the tasks
                 if (departure_barrier) {
                     for (FJTask t : task.job.tasks) {
                         t.worker.current_task = null;
                     }
                 }
                 
                 // it is the last, record the completion time
                 task.job.completion_time = time;

                 // if this is the first job to complete, we still need to allocate the pre-departure
                 // overhead process.  This assumes that the number of tasks per job will stay the same
                 if (pre_departure_overhead_process == null) {
                     pre_departure_overhead_process = new ConstantIntertimeProcess(job_predeparture_overhead + task.job.num_tasks*task_predeparture_overhead, true);
                 }

                 // the overhead model includes pre-departure overhead that we implement by scheduling 
                 // a final dummy task whose runtime is the pre-departure overhead.
                 dummy_overhead_task_queue.add(new FJTask(pre_departure_overhead_process, time, task.job));
                 if (dummy_overhead_worker.current_task == null) {
                     serviceTask(dummy_overhead_worker, dummy_overhead_task_queue.poll(), time);
                 }

             } else {
                 // this server type does not queue tasks on the workers
                 //serviceTask(worker, worker.queue.poll(), time);
             }
         }
         
         // service the next job, if possible, if any
         feedWorkers(time);
     }
     
     
    @Override
    public int queueLength() {
        return this.job_queue.size();
    }

}
