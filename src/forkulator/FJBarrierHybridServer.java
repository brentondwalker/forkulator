package forkulator;

import forkulator.randomprocess.IntertimeProcess;
import forkulator.randomprocess.UniformIntertimeProcess;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class FJBarrierHybridServer extends FJServer {

	/**
     * This server is like the FJBarrierServer, but it serves a mix of BEM jobs
     * and standard single-queue fork-join jobs (FJSingleQueueServer).
     * Consider the case of BEM jobs with start barriers.  When such a job
     * starts service, it blocks the available workers until k workers are
     * available and its tasks can start in unison.  When a non-BEM job begins
     * service, its tasks are assigned to workers and begin executing immediately.
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

    boolean current_job_bem = true;
    IntertimeProcess uniform_random = new UniformIntertimeProcess(0.0, 1.0);

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
     * The Bernoulli probability that any job is a barrier job.
     * Implement the distinction here so the changes can be confined to this FJServer class.
     * Otherwise we'd need different FJJob subclasses, or a flag in FJJob.
     * XXX In the future, we may want the BEM probability to be something other than Bernoulli.
     * XXX The case that this kind of implementation can't support, is the case where
     *     BEM jobs and non-BEM jobs are fed by different random processes.
     */
    private double bem_rate = 1.0;

    /**
     * Constructor
     *
     * Has to allocate the workers' task queues.
     *
     * @param num_workers
     * @param start_barrier
     * @param departure_barrier
     * @param bem_rate    the Bernoulli probability that any job is a BEM job
     *
     */
    public FJBarrierHybridServer(int num_workers, boolean start_barrier, boolean departure_barrier, double bem_rate) {
        super(num_workers);
        if (bem_rate > 1.0 || bem_rate < 0.0) {
            System.err.println("ERROR: FJBarrierHybridServer bem_rate must be between 0 and 1");
            System.exit(0);
        }

        this.departure_barrier = departure_barrier;
        this.start_barrier = start_barrier;
        this.bem_rate = bem_rate;
        System.err.println("FJBarrierHybridServer(departure_barrier="+departure_barrier+" , start_barrier="+start_barrier+", bem_rate="+bem_rate+")");
    }


    /**
     * Constructor
     *
     * Has to allocate the workers' task queues.
     *
     * @param num_workers
     */
    public FJBarrierHybridServer(int num_workers) {
        super(num_workers);
        System.err.println("FJBarrierHybridServer()");
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
    	 if (current_job == null) return;

         // count the number of idle workers
         Vector<Integer> idle_workers = new Vector<Integer>();
         for (int w = 0; w < num_workers; w++) {
             if (workers[0][w].current_task == null) {
                 idle_workers.add(w);
             }
         }
         if (idle_workers.isEmpty()) return;

        // handle BEM jobs and non-BEM jobs differently
        if (current_job_bem) {  // BEM case
             // can we service this job?
            //System.err.println("feedWorkers(BEM)");
             if ((idle_workers.size() >= current_job.num_tasks) || (idle_workers.size() > 0 && start_barrier == false)) {
                 int w = 0;
                 FJTask nt = current_job.nextTask();
                 while (nt != null && w < idle_workers.size()) {
                     serviceTask(workers[0][idle_workers.get(w)], nt, time);
                     nt = current_job.nextTask();
                     w++;
                 }
                 current_job = job_queue.poll();
                 current_job_bem = (uniform_random.nextInterval() <= bem_rate);
                 feedWorkers(time);
             }
        } else {  // non-BEM case
            //System.err.println("feedWorkers(NON-BEM)");
            for (int w=0; w<num_workers; w++) {
                 if (workers[0][w].current_task == null) {
                     // service the next task
                     serviceTask(workers[0][w], current_job.nextTask(), time);
                     // if the current job is exhausted, grab a new one (or null)
                     if (current_job.fully_serviced) {
                         current_job = job_queue.poll();
                         current_job_bem = (uniform_random.nextInterval() <= bem_rate);
                         // we have to exit this loop, even if there are idle workers,
                         // because the next job may have barriers.
                         break;
                     }
                 }
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
         
         if (current_job == null) {
        	 current_job = job;
             current_job_bem = (uniform_random.nextInterval() <= bem_rate);
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
         } else {
        	 // this server type does not queue tasks on the workers
             //serviceTask(worker, worker.queue.poll(), time);
         }
         
         // service the next job, if possible, if any
         feedWorkers(time);
     }
     
     
	@Override
	public int queueLength() {
		if (current_job == null) {
			return 0;
		}
        return this.job_queue.size() + 1;
	}

}
