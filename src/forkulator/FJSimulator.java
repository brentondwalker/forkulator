package forkulator;

import java.util.LinkedList;
import java.util.Random;

public class FJSimulator {

	private static Random rand = new Random();
	
	public LinkedList<QEvent> event_queue = new LinkedList<QEvent>();
	
	public int num_workers;
	public int num_tasks;
	public double arrival_rate;
	public double service_rate;
	public FJServer server = null;
	
	public FJSimulator(int num_workers, int num_tasks, double arrival_rate, double service_rate) {
		this.num_workers = num_workers;
		this.num_tasks = num_tasks;
		this.arrival_rate = arrival_rate;
		this.service_rate = service_rate;
		
		this.server = new FJServer(num_workers);
		
		FJServer.setSimulator(this);
	}
	
	public void run(int num_jobs) {
		// we can generate all the job arrivals at once
		double current_time = 0.0;
		for (int i=0; i<num_jobs; i++) {
			double next_time = current_time + -Math.log(rand.nextDouble())/service_rate;
			event_queue.add(new QJobArrivalEvent(next_time));
			current_time = next_time;
		}
		
		// start processing events
		while (! event_queue.isEmpty()) {
			QEvent e = event_queue.removeFirst();
			
			if (e instanceof QJobArrivalEvent) {
				QJobArrivalEvent et = (QJobArrivalEvent) e;
				FJJob job = new FJJob(num_tasks, service_rate);
				job.arrival_time = et.time;
				server.enqueJob(job);
			} else if (e instanceof QTaskCompletionEvent) {
				QTaskCompletionEvent et = (QTaskCompletionEvent) e;
				server.taskCompleted(et.task.worker, et.time);
			}
		}
		
	}
	
	public void addEvent(QEvent e) {
		if (event_queue.isEmpty()) {
			event_queue.add(e);
		} else {
			int i = 0;
			for (QEvent le : this.event_queue) {
				if (le.time > e.time) {
					event_queue.add(i, e);
					System.out.println("inserting event with time "+e.time+" before event "+i+" with time "+le.time);
					break;
				}
				i++;
			}
		}
	}
	
	/**
	 * main()
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		int num_workers = Integer.parseInt(args[0]);
		int num_tasks = Integer.parseInt(args[1]);
		double arrival_rate = Double.parseDouble(args[2]);
		double service_rate = Double.parseDouble(args[3]);
		int num_jobs = Integer.parseInt(args[4]);
		
		FJSimulator sim = new FJSimulator(num_workers, num_tasks, arrival_rate, service_rate);
		sim.run(num_jobs);
	}

}
