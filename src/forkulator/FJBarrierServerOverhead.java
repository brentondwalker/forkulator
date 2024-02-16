package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

import forkulator.randomprocess.ConstantIntertimeProcess;
import forkulator.randomprocess.IntertimeProcess;

public class FJBarrierServerOverhead extends FJServer {

    /**
     * This will be like the previous class, but with the 4-parameter model for overhead.
     * The task service overhead could be incorporated into the service IntertimeProcess.
     * The job and task pre-departure overhead has to be implemented in this class.
     * 
     * This is following the pattern used in FJHierarchicalSplitMergeServerOverhead which 
     * we used in the TinyTasks extended paper.
     */
    
    public double job_predeparture_overhead = 0.0;

    public double task_predeparture_overhead = 0.0;

    public IntertimeProcess pre_departure_overhead_process = null;

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

    FJJob next_job = null;
    
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
    private boolean start_barrier = true;
    
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJBarrierServerOverhead(int num_workers, boolean start_barrier, boolean departure_barrier, double job_predeparture_overhead, double task_predeparture_overhead) {
        super(num_workers);
        this.departure_barrier = departure_barrier;
        this.start_barrier = start_barrier;
        this.job_predeparture_overhead = job_predeparture_overhead;
        this.task_predeparture_overhead = task_predeparture_overhead;
        this.dummy_overhead_worker = new FJWorker();
        System.err.println("FJBarrierServerOverhead(departure_barrier="+departure_barrier+" , start_barrier="+start_barrier+")"
                +", job_predeparture_overhead="+job_predeparture_overhead+", task_predeparture_overhead="+task_predeparture_overhead+")");
    }
    
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJBarrierServerOverhead(int num_workers, double job_predeparture_overhead, double task_predeparture_overhead) {
        super(num_workers);
        this.job_predeparture_overhead = job_predeparture_overhead;
        this.task_predeparture_overhead = task_predeparture_overhead;
        this.dummy_overhead_worker = new FJWorker();
        System.err.println("FJBarrierServerOverhead("+"job_predeparture_overhead="+job_predeparture_overhead+", task_predeparture_overhead="+task_predeparture_overhead+")");
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
         if (next_job == null) {
             return;
         }
         
         // count the number of idle workers
         Vector<Integer> idle_workers = new Vector<Integer>();
         for (int w=0; w<num_workers; w++) {
             if (workers[0][w].current_task == null) {
                 idle_workers.add(w);
             }
         }
         
         // can we service this job?
         if ((idle_workers.size() >= next_job.num_tasks) || (idle_workers.size() > 0 && start_barrier==false))  {
             int w = 0;
             FJTask nt = next_job.nextTask();
             while (nt != null && w < idle_workers.size()) {
                 serviceTask(workers[0][idle_workers.get(w)], nt, time);
                 nt = next_job.nextTask();
                 w++;
             }
             next_job = job_queue.poll();
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
         
         if (next_job == null) {
             next_job = job;
             feedWorkers(job.arrival_time);
         } else {
             job_queue.add(job);
         }
         if (FJSimulator.DEBUG) System.out.println("  queued a job.    New queue length: "+job_queue.size());
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

         // check if this was the end of the pre-departure overhead dummy task
         if (task.job.completed && worker==dummy_overhead_worker) {
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
             //TODO: this could be more efficient
             boolean compl = true;
             for (FJTask t : task.job.tasks) {
                 compl = compl && t.completed;
             }
             task.job.completed = compl;

             if (task.job.completed) {
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
        if (next_job == null) {
            return 0;
        }
        return this.job_queue.size() + 1;
    }

}
