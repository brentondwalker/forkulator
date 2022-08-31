package forkulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import forkulator.randomprocess.ConstantIntertimeProcess;
import forkulator.randomprocess.IntertimeProcess;

public class FJHierarchicalSplitMergeServerOverhead extends FJServer {

    /**
     * This will be like the previous class, but with the 4-parameter model for overhead.
     * The task service overhead could be incorporated into the service IntertimeProcess,
     * though we don't have the right thing implemented yet.  
     * The job and task pre-departure overhead has to be implemented in this class.
     * 
     * Initially we're just interested in the Split-Merge behavior.
     * This is to simulate hierarchical parallel systems like Slurm/MPI/OpenMP.  
     * In these cases Slurm/MPI receives a job and starts up a
     * bunch of processes to service it.  Each process spawns many OpenMP threads which
     * are used to service the actual tasks and do work-stealing.  We'll simulate the
     * OpenMP work-stealing part as a centralized single queue.
     * 
     * This starts to break the parent class FJServer a bit, since the workers are
     * grouped into separate processes.  FJServer keeps them all together.  Here we will
     * keep parallel structures, but still use the ones defined in FJServer.
     */
    
    public int num_processes = 1;
    
    public int num_threads_per_process = 1;
    
    public double job_predeparture_overhead = 0.0;
    
    public double task_predeparture_overhead = 0.0;
    
    public IntertimeProcess pre_departure_overhead_process = null;
    
    /**
     * To implement pre-departure overhead, once all the tasks finish and
     * the job is marked "complete", we queue a dummy task on a dummy worker
     * that has a fixed execution time equal to the desired pre-departure
     * overhead.  When this task completes, then the job can depart.
     */
    public FJTask dummy_overhead_task = null;
    public FJWorker dummy_overhead_worker = null;
    
    public Queue<FJJob> job_queue = new LinkedList<FJJob>();

    public Queue<FJTask> process_task_queue[] = null;
    
    public FJWorker process_threads[][] = null;
    
    /**
     * Tables keep track of the process and thread index of each worker.
     */
    public HashMap<FJWorker,Integer> worker_to_process = null;
    public HashMap<FJWorker,Integer> worker_to_thread = null;
    
    FJJob next_job = null;
        
    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJHierarchicalSplitMergeServerOverhead(int num_processes, int num_threads_per_process, double job_predeparture_overhead, double task_predeparture_overhead) {
        super(num_processes * num_threads_per_process);  // also initializes num_workers
        System.err.println("FJHierarchicalSplitMergeServerOverhead(num_processes="+num_processes+", num_threads_per_process="+num_threads_per_process
                +", job_predeparture_overhead="+job_predeparture_overhead+", task_predeparture_overhead="+task_predeparture_overhead+")");
        
        this.num_processes = num_processes;
        this.num_threads_per_process = num_threads_per_process;
        this.job_predeparture_overhead = job_predeparture_overhead;
        this.task_predeparture_overhead = task_predeparture_overhead;
        //this.pre_departure_overhead_process = new ConstantIntertimeProcess(job_predeparture_overhead + num_processes*num_threads_per_process*task_predeparture_overhead, true);
        this.dummy_overhead_worker = new FJWorker();
        this.process_task_queue = (Queue<FJTask>[]) new LinkedList<?>[num_processes];  // absolute hack but necessary?  https://stackoverflow.com/questions/217065/cannot-create-an-array-of-linkedlists-in-java
        for (int p=0; p<num_processes; p++) {
            process_task_queue[p] = new LinkedList<FJTask>();
        }
        // assign the workers (OpenMP threads) to the worker groups (MPI processes)
        process_threads = new FJWorker[num_processes][num_threads_per_process];
        worker_to_process = new HashMap<FJWorker,Integer>();
        worker_to_thread = new HashMap<FJWorker,Integer>();
        int process = 0;
        int thread = 0;
        for (int i=0; i<num_workers; i++) {
            process = i / num_threads_per_process;
            thread = i % num_threads_per_process;

            process_threads[process][thread] = workers[0][i];
            if (FJSimulator.DEBUG) System.out.println("process_threads["+process+"]["+thread+"]  set to "+workers[0][i]);

            worker_to_process.put(workers[0][i], process);
            worker_to_thread.put(workers[0][i], thread);
        }
    }

    
    /**
     * Constructor
     * 
     * Has to allocate the workers' task queues.
     * 
     * @param num_workers
     */
    public FJHierarchicalSplitMergeServerOverhead(int num_workers) {
        this(num_workers, 1, 0.0, 0.0);
        System.err.println("FJHierarchicalSplitMergeServerOverhead()");
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

         // can we service this job?
         // the previous job must be departed
         if (current_job == null) {
             current_job = next_job;
             next_job = job_queue.poll();

             // distribute the tasks to the process queues
             FJTask nt = current_job.nextTask();
             int proc = 0;
             while (nt != null) {
                 process_task_queue[proc].add(nt);
                 if (FJSimulator.DEBUG) System.out.println("queue task "+nt+" on process "+proc+"  "+time);
                 nt = current_job.nextTask();
                 proc = (proc + 1) % num_processes;
             }
             
             feedWorkers(time);
         }
         
         // go through the process queues trying to service tasks
         for (int i=0; i<this.num_processes; i++) {
             Queue<FJTask> q = process_task_queue[i];
             
             for (FJWorker worker : process_threads[i]) {
                 if (q.isEmpty()) { break; }
                 if (worker.current_task == null) {
                     serviceTask(worker, q.poll(), time);
                     if (FJSimulator.DEBUG) System.out.println("service task "+worker.current_task+" on worker "+this.worker_to_process.get(worker)+":"+this.worker_to_thread.get(worker)+"  "+time);

                 }
             }
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
      * check if this process has another task to service.
      * 
      * @param workerId
      * @param time
      */
     public void taskCompleted(FJWorker worker, double time) {
         if (FJSimulator.DEBUG) System.out.println("worker "+this.worker_to_process.get(worker)+":"+this.worker_to_thread.get(worker)+"  task "+worker.current_task.path_log_id+" completed "+time);
         FJTask task = worker.current_task;
         task.completion_time = time;
         task.completed = true;
         
         worker.current_task = null;
         
         // check if this was the end of the pre-departure overhead dummy task
         if (task.job.completed && task==dummy_overhead_task) {
             
             // departure time will be different from completion time
             task.job.departure_time = time;
             
             // split-merge behavior, only service one job at a time
             this.current_job = null;
             
             // sample and dispose of the job
             if (FJSimulator.DEBUG) System.out.println("job departing: "+task.job.path_log_id+"\t"+time);
             jobDepart(task.job);
             
         } else {
             // check if this task was the last one of a job
             //TODO: this could be more efficient
             boolean compl = true;
             for (FJTask t : task.job.tasks) {
                 compl = compl && t.completed;
             }
             task.job.completed = compl;
             
             if (task.job.completed) {
                 // we should not have to clear out the tasks from the workers,
                 // that is done as the individual tasks complete.
                 if (FJSimulator.DEBUG) System.out.println("job complete: "+task.job.path_log_id+"\t"+time);
                 
                 // it is the last, record the completion time
                 task.job.completion_time = time;
                 
                 // if this is the first job to complete, we still need to allocate the pre-departure
                 // overhead process.  This assumes that the number of tasks per job will stay the same
                 if (pre_departure_overhead_process == null) {
                     pre_departure_overhead_process = new ConstantIntertimeProcess(job_predeparture_overhead + task.job.num_tasks*task_predeparture_overhead, true);
                 }
                 
                 // the overhead model includes pre-departure overhead that we implement by scheduling 
                 // a final dummy task whose runtime is the pre-departure overhead.
                 dummy_overhead_task = new FJTask(pre_departure_overhead_process, time, task.job);
                 serviceTask(dummy_overhead_worker, dummy_overhead_task, time);
             } else {
                 int my_process = worker_to_process.get(worker);
                 serviceTask(worker, process_task_queue[my_process].poll(), time);
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
