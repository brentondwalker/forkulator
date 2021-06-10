package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class FJBarrierServer extends FJServer {

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
    public FJBarrierServer(int num_workers, boolean start_barrier, boolean departure_barrier) {
        super(num_workers);
        this.departure_barrier = departure_barrier;
        this.start_barrier = start_barrier;
        System.err.println("FJBarrierServer(departure_barrier="+departure_barrier+" , start_barrier="+start_barrier+")");
    }

    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJBarrierServer(int num_workers) {
        super(num_workers);
        System.err.println("FJBarrierServer()");
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
		if (next_job == null) {
			return 0;
		}
        return this.job_queue.size() + 1;
	}

}
