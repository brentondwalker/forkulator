package forkulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
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
public class FJPathLogger {
		
	// the number of jobs of the path to save
	int numjobs = 0;
	
	// keep track of how many jobs we've logged
	int job_index = 0;
	
	// this writer is opened when the class is instantiated, and written to as data is added
	BufferedWriter writer = null;
	
	// the jobs we record path data for (in the order of arrival)
	public FJJob[] jobs = null;
	
	
	/**
	 * constructor
	 * 
	 * @param numjobs
	 */
	public FJPathLogger(int numjobs) {
		this.numjobs = numjobs;
		jobs = new FJJob[numjobs];
	}
	
	/**
	 * Call this when the job arrives.
	 * We assume this is called on job in the order that they arrive.
	 * 
	 * @param task
	 */
	public void addJob(FJJob job) {
		if (job_index < numjobs) {
			jobs[job_index++] = job;
			job.setPatlog(true);
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
		
		if (jobs == null) {
			System.err.println("WARNING: tried to save a pathlog, but none was recorded");
			return;
		}
		
		try {
			if (compress) {
				outfile = outfile+".gz";
				GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(outfile)));
				writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
			} else {
				writer = new BufferedWriter(new FileWriter(outfile));
			}
			
			int task_count = 0;
			int job_count = 0;
			
			for (FJJob job : jobs) {
				// it's possible we didn't record as many jobs as requested.
				// if so, just print out what we have
				if (job == null) {
					break;
				}
				double job_start_time = job.tasks[0].start_time;
				double job_departure_time = job.departure_time;
				double job_completion_time = job.completion_time;
				for (FJTask task : job.tasks) {
					job_start_time = Math.min(job_start_time, task.start_time);
					job_completion_time = Math.max(job_completion_time, task.completion_time);
				}
				double job_sojourn = job_departure_time - job.arrival_time;
				if (job_sojourn > 10000) {
					System.err.println("WARNING: large job sojourn: "+job_sojourn);
					System.err.println("departure: "+job_departure_time);
					System.err.println("arrival:    "+job.arrival_time);
					System.exit(1);
				}
				
				for (FJTask task : job.tasks) {
					writer.write(task_count++
							+"\t"+job_count
							+"\t"+job.arrival_time
							+"\t"+job_start_time
							+"\t"+job_completion_time
							+"\t"+job_departure_time
							+"\t"+task.start_time
							+"\t"+task.completion_time
							+"\n");
				}
				job_count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// Close the writer regardless of what happens...
				writer.close();
			} catch (Exception e) {
			}
		}
	}
	
}
