package forkulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.zip.GZIPOutputStream;

/**
 * This class automates the saving of part of the experiment path.
 * The path of these queueing experiments consists of the following for each task:
 * - task id
 * - job id
 * - job arrival_time
 * - job start_time
 * - job completion_time
 * - job departure_time
 * - task start_time
 * - task completion_time
 * 
 * We used to give each job and task sequential ID numbers, but I removed that because
 * it was using static variables and was not compatible when running on Spark.
 * Now it adds some complexity to this class, that we have to assign our own
 * sequential ID numbers to the jobs and tasks as they come.
 * 
 * Because of that, and because jobs can depart out of order, we can't just write
 * out the data as the jobs depart.  We need to save up all the jobs and then
 * sort them by their arrival time, and then write out the task info.
 * We need to it this way anyway to support path logging on Spark.
 * 
 * TODO: extend this to record internal details for multi-stage systems
 * 
 * @author brenton
 *
 */
public class FJPathLogger implements Serializable {
	
	/**
	 * Supposed to add this to say the class implements Serializable.
	 */
	private static final long serialVersionUID = 1L;

	// this is basically a struct
	class TaskDatum {
		int task_id = 0;
		int job_id = 0;
		double job_arrival_time = 0.0;
		double job_start_time = 0.0;
		double job_departure_time = 0.0;
		double job_completion_time = 0.0;
		double task_start_time = 0.0;
		double task_completion_time = 0.0;
	}
	
	// the number of jobs of the path to save
	int numjobs = 0;
	
	// number of tasks per job
	int tasks_per_job = 0;
	
	// keep track of how many jobs and tasks we've logged
	int job_index = 0;
	int task_index = 0;
	
	// the path data we recorded
	private TaskDatum[] task_data = null;
	
	
	/**
	 * constructor
	 * 
	 * @param numjobs
	 */
	public FJPathLogger(int numjobs, int tasks_per_job) {
		this.numjobs = numjobs;
		this.tasks_per_job = tasks_per_job;
		task_data = new TaskDatum[numjobs*tasks_per_job];
	}
	
	/**
	 * Call this when the job arrives.
	 * We assume this is called on job in the order that they arrive.
	 * 
	 * @param task
	 */
	public void addJob(FJJob job) {
		if (job_index < numjobs) {
			job.path_log_id = job_index++;
		}
	}
	
	/**
	 * Call this when a job departs and has its data sampled.
	 * 
	 * @param job
	 */
	public void recordJob(FJJob job) {
		if (job.path_log_id >= 0) {
			double job_start_time = job.tasks[0].start_time;
			for (int i=0; i<job.num_tasks; i++) {
				job_start_time = Math.min(job_start_time, job.tasks[i].start_time);
			}
			for (int i=0; i<job.num_tasks; i++) {
				task_data[task_index] = new TaskDatum();
				task_data[task_index].task_id = task_index;
				task_data[task_index].job_id = job.path_log_id;
				task_data[task_index].job_arrival_time = job.arrival_time;
				task_data[task_index].job_start_time = job_start_time;
				task_data[task_index].job_completion_time = job.completion_time;
				task_data[task_index].job_departure_time = job.departure_time;
				task_data[task_index].task_completion_time = job.tasks[i].completion_time;
				task_data[task_index].task_start_time = job.tasks[i].start_time;
				task_index++;
			}
		}
	}
	
	/**
	 * Write out this pathlog's data to a file.
	 * 
	 * @param outfile_base
	 * @param compress
	 */
	public void writePathlog(String outfile_base, boolean compress) {
		String outfile = outfile_base+"_path.dat";
		
		if (task_data == null) {
			System.err.println("WARNING: tried to save a pathlog, but none was recorded");
			return;
		}
		
		BufferedWriter writer = null;
		
		try {
			if (compress) {
				outfile = outfile+".gz";
				GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(outfile)));
				writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
			} else {
				writer = new BufferedWriter(new FileWriter(outfile));
			}
			
			for (TaskDatum td : task_data) {
				// it's possible we didn't record as many jobs as requested.
				// if so, just print out what we have
				if (td == null) {
					break;
				}
				
				writer.write(td.task_id++
						+"\t"+td.job_id
						+"\t"+td.job_arrival_time
						+"\t"+td.job_start_time
						+"\t"+td.job_completion_time
						+"\t"+td.job_departure_time
						+"\t"+td.task_start_time
						+"\t"+td.task_completion_time
						+"\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// Close the writer regardless of what happens...
				writer.close();
			} catch (Exception e) {
				System.err.println("ERROR: everything has gone wrong\n"+e);
			}
		}
	}
	
}
