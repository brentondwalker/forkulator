package forkulator;

import java.util.LinkedList;
import java.util.Queue;

public class FJServer {

	private static FJSimulator simulator = null;
	
	public int num_workers = 0;
	public FJWorker[] workers = null;
	public Queue<FJJob> job_queue = new LinkedList<FJJob>();
	public FJJob current_job = null;
	
	public FJServer(int num_workers) {
		this.num_workers = num_workers;
		this.workers = new FJWorker[num_workers];
		for (int i=0; i<num_workers; i++) {
			workers[i] = new FJWorker();
		}
	}
	
	public static void setSimulator(FJSimulator simulator) {
		FJServer.simulator = simulator;
	}
	
	public void feedWorkers(double time) {
		// check for idle workers
		for (int i=0; i<num_workers; i++) {

			// if there is no current job, just return
			if (current_job == null) return;
			
			if (workers[i].current_task == null) {
				// service the next task
				workers[i].current_task = current_job.nextTask();
				workers[i].current_task.worker = i;
				workers[i].current_task.start_time = time;
				workers[i].current_task.processing = true;
				
				// schedule the task's completion
				QTaskCompletionEvent e = new QTaskCompletionEvent(workers[i].current_task,
						time + workers[i].current_task.service_time);
				simulator.addEvent(e);
				
				// if the current job is exhausted, grab a new one (or null)
				if (current_job.complete) {
					current_job = job_queue.poll();
				}
			}
		}
	}
	
	public void enqueJob(FJJob job) {
		System.out.println("enqueJob() "+job.arrival_time);
		if (current_job == null) {
			current_job = job;
		} else {
			job_queue.add(job);
			feedWorkers(job.arrival_time);
		}
	}
	
	public void taskCompleted(int workerId, double time) {
		System.out.println("task "+workers[workerId].current_task.ID+" completed "+time);
		workers[workerId].current_task.completion_time = time;
		workers[workerId].current_task.completed = true;
		
		// if there is no current job, just clear the worker
		if (current_job == null) {
			workers[workerId].current_task = null;
			return;
		}
		
		// put a new task on the worker
		workers[workerId].current_task = current_job.nextTask();
		workers[workerId].current_task.worker = workerId;
		workers[workerId].current_task.start_time = time;
		workers[workerId].current_task.processing = true;
		
		// schedule the task's completion
		QTaskCompletionEvent e = new QTaskCompletionEvent(workers[workerId].current_task,
				time + workers[workerId].current_task.service_time);
		simulator.addEvent(e);
		
		// if the current job is exhausted, grab a new one (or null)
		if (current_job.complete) {
			current_job = job_queue.poll();
		}
	}
	
	
}
