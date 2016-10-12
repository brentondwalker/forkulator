package forkulator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
//import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * Simulate fork-join systems.
 * 
 * java -Xmx5g -cp "lib/commons-math3-3.6.1.jar:bin" forkulator.FJSimulator w 1 1 0.7 1.0 100000000 1000 fjpaper-data/mm1boundsmu1lambda07.dat
 * 
 * sbt assembly
 * 
 * sbt "runMain forkulator.FJSimulator -q w -A x 0.5 -S x 1.0 -w 10 -t 10 -i 1 -n 1000 -o testrun"
 * 
 * ./bin/spark-submit --master spark://172.23.27.20:7077  --conf spark.cores.max=2 --class forkulator.FJSimulator /home/brenton/properbounds/forkulator-sbt/target/scala-2.10/forkulator-assembly-1.0.jar -q w -A x 0.5 -S x 1.0 -w 10 -t 10 -i 1 -n 10000000 -o testrun
 * 
 * @author brenton
 *
 */
public class FJSimulator {

	public static boolean DEBUG = false;
	public static final int QUEUE_STABILITY_THRESHOLD = 1000000;
	public static final int SAMPLE_PATH_LENGTH = 1000;
	
	public LinkedList<QEvent> event_queue = new LinkedList<QEvent>();
	
	public int num_workers;
	public int num_tasks;
	public IntertimeProcess arrival_process;
	public IntertimeProcess service_process;
	public FJServer server = null;
	
	public double binwidth = 0.1;
	public double quanile_epsilon = 1e-6;
	
	public static FJDataAggregator data_aggregator = null;
	
	
	/**
	 * constructor
	 * 
	 * @param num_workers
	 * @param num_tasks
	 * @param arrival_rate
	 * @param service_rate
	 */
	public FJSimulator(String server_queue_type, int num_workers, int num_tasks, IntertimeProcess arrival_process, IntertimeProcess service_process, FJDataAggregator data_aggregator) {
		this.num_workers = num_workers;
		this.num_tasks = num_tasks;
		this.arrival_process = arrival_process;
		this.service_process = service_process;
		this.data_aggregator = data_aggregator;
		
		if (server_queue_type.toLowerCase().equals("s")) {
			this.server = new FJSingleQueueServer(num_workers);
		} else if (server_queue_type.toLowerCase().equals("w")) {
			this.server = new FJWorkerQueueServer(num_workers);
		} else if (server_queue_type.toLowerCase().startsWith("td")) {
			if (server_queue_type.length() == 3 && server_queue_type.toLowerCase().equals("tdr")) {
				this.server = new FJThinningServer(num_workers, false, true);  // resequencing
			} else {
				this.server = new FJThinningServer(num_workers, false, false);
			}
		} else if (server_queue_type.toLowerCase().startsWith("tr")) {
			if (server_queue_type.length() == 3 && server_queue_type.toLowerCase().equals("tdr")) {
				this.server = new FJThinningServer(num_workers, true, true);  // resequencing
			} else {
				this.server = new FJThinningServer(num_workers, true, false);
			}
		} else if (server_queue_type.toLowerCase().startsWith("wkl")) {
			int l_diff = Integer.parseInt(server_queue_type.toLowerCase().substring(3));
			this.server = new FJKLWorkerQueueServer(num_workers, num_workers - l_diff);
		} else if (server_queue_type.toLowerCase().startsWith("skl")) {
			int l_diff = Integer.parseInt(server_queue_type.toLowerCase().substring(3));
			this.server = new FJKLSingleQueueServer(num_workers, num_workers - l_diff);
		} else if (server_queue_type.toLowerCase().startsWith("msw")) {
			// multi-stage worker-queue
			if (server_queue_type.toLowerCase().startsWith("mswi")) {
				// independent service times at each stage
				int num_stages = Integer.parseInt(server_queue_type.toLowerCase().substring(4));
				this.server = new FJMultiStageWorkerQueueServer(num_workers, num_stages, true);
			} else {
				// otherwise service times of each task stay the same across all stages
				int num_stages = Integer.parseInt(server_queue_type.toLowerCase().substring(3));
				this.server = new FJMultiStageWorkerQueueServer(num_workers, num_stages, false);
			}
		} else {
			System.err.println("ERROR: unknown server queue type: "+server_queue_type);
			System.exit(1);
		}
		FJServer.setSimulator(this);
	}
	
	
	/**
	 * put the initial events in the simulation queue and start processing them
	 * 
	 * @param num_jobs
	 */
	public void run(long num_jobs, int sampling_interval) {
		// before we generated all the job arrivals at once
		// now to save space we only have one job arrival in the queue at a time
		event_queue.add(new QJobArrivalEvent(arrival_process.nextInterval()));
		
		// start processing events
		int sampling_countdown = sampling_interval;
		long jobs_processed = 0;
		while (! event_queue.isEmpty()) {
			if (this.server.queueLength() > FJSimulator.QUEUE_STABILITY_THRESHOLD) {
				System.err.println("ERROR: queue exceeded threshold.  The system is unstable.");
				System.exit(0);
			}
			
			QEvent e = event_queue.removeFirst();
			
			if (e instanceof QJobArrivalEvent) {
				jobs_processed++;
				if (((jobs_processed*100)%num_jobs)==0)
					System.err.println("   ... "+(100*jobs_processed/num_jobs)+"%");
				QJobArrivalEvent et = (QJobArrivalEvent) e;
				FJJob job = new FJJob(num_tasks, service_process, e.time);
				job.arrival_time = et.time;
				if (sampling_countdown==0) {
					server.enqueJob(job, true);
					sampling_countdown = sampling_interval;
				} else {
					server.enqueJob(job, false);
				}
				sampling_countdown--;
				
				// schedule the next job arrival
				if (jobs_processed < num_jobs) {
					double interval = arrival_process.nextInterval();
					//if ((interval < 0.0) || (interval>1000)) {
					//	System.err.println("WARNING: inter-arrival time of "+interval);
					//}
					double next_time = et.time + interval;
					this.addEvent(new QJobArrivalEvent(next_time));
				}
			} else if (e instanceof QTaskCompletionEvent) {
				QTaskCompletionEvent et = (QTaskCompletionEvent) e;
				server.taskCompleted(et.task.worker, et.time);
			}
		}
	}
	
	
	/**
	 * add an event to the simulation queue
	 * 
	 * @param e
	 */
	public void addEvent(QEvent e) {
		int queue_len = this.event_queue.size();
		if (event_queue.isEmpty()) {
			event_queue.add(e);
		} else if (e.time > this.event_queue.getLast().time) {
			event_queue.add(e);
		} else {
			int i = 0;
			for (QEvent le : this.event_queue) {
				if (le.time >= e.time) {
					event_queue.add(i, e);
					if (DEBUG) System.out.println("inserting event with time "+e.time+" before event "+i+" with time "+le.time);
					break;
				}
				i++;
			}
		}
		
		// do a sanity check
		if ((this.event_queue.size() - queue_len) != 1) {
			System.err.println("ERROR: adding one thing resulted in wrong change to queue len: "+this.event_queue.size()+"  "+queue_len);
			System.err.println("new event time: "+e.time);
			for (QEvent le : this.event_queue) {
				System.err.println(le.time);
			}
		}
		double last_time = 0.0;
		for (QEvent le : this.event_queue) {
			if (le.time < last_time) {
				System.err.println("ERROR: events in queue out of order!");
				System.exit(1);
			}
			last_time = le.time;
		}
	}
	
	
	
	
	/**
	 * compute the temporal autocorrelation of the job statistics
	 * 
	 * @param outfile_base
	 * @param max_offset
	 */
	/*
	public void jobAutocorrelation(String outfile_base, int max_offset) {
		double[] sojourns = new double[server.sampled_jobs.size()];
		double[] waitings = new double[server.sampled_jobs.size()];
		double[] services = new double[server.sampled_jobs.size()];
		int total = server.sampled_jobs.size();
		int i = 0;
		for (FJJob job : server.sampled_jobs) {
			double job_start_time = job.tasks[0].start_time;
			double job_completion_time = job.tasks[0].completion_time;
			for (FJTask task : job.tasks) {
				job_start_time = Math.min(job_start_time, task.start_time);
				job_completion_time = Math.max(job_completion_time, task.completion_time);
			}
			sojourns[i] = job_completion_time - job.arrival_time;
			waitings[i] = job_start_time - job.arrival_time;
			services[i] = job_completion_time - job_start_time;
			i++;
		}
		
		// first we need the means
		double sojourn_sum = 0.0;
		double waiting_sum = 0.0;
		double service_sum = 0.0;
		for (int j=0; j<sojourns.length; j++) {
			sojourn_sum += sojourns[j];
			waiting_sum += waitings[j];
			service_sum += services[j];
		}
		
		double sojourn_mean = sojourn_sum/total;
		double waiting_mean = waiting_sum/total;
		double service_mean = service_sum/total;
		
		// now compute autocorr
		double[] sojourn_ac = new double[max_offset];
		double[] waiting_ac = new double[max_offset];
		double[] service_ac = new double[max_offset];
		for (int j=0; j<(total-max_offset); j++) {
			for (int x=0; x<max_offset; x++) {
				sojourn_ac[x] += (sojourns[j]-sojourn_mean)*(sojourns[j+x]-sojourn_mean);
				waiting_ac[x] += (waitings[j]-waiting_mean)*(waitings[j+x]-waiting_mean);
				service_ac[x] += (services[j]-service_mean)*(services[j+x]-service_mean);
			}
		}
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_autocor.dat"));
			for (int x=0; x<max_offset; x++) {
				writer.write(x
						+"\t"+sojourn_ac[x]/(total-max_offset)
						+"\t"+waiting_ac[x]/(total-max_offset)
						+"\t"+service_ac[x]/(total-max_offset)
						+"\n");
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
	*/
	
	
	/**
	 * Print out the experiment path (if it's small enough) and the CDFs and PDFs
	 * of sojourn, waiting, and service times
	 * 
	 * @param outfile_base
	 */
	public void printExperimentPath(String outfile_base) {
		
		// max value for the distributions
		double max_value = 0;
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(outfile_base+"_path.dat"));
			for (FJJob job : server.sampled_jobs) {
				double job_start_time = job.tasks[0].start_time;
				double job_departure_time = job.departure_time;
				double job_completion_time = job.completion_time;
				for (FJTask task : job.tasks) {
					job_start_time = Math.min(job_start_time, task.start_time);
				}
				double job_sojourn = job_departure_time - job.arrival_time;
				if (job_sojourn > 10000) {
					System.err.println("WARNING: large job sojourn: "+job_sojourn);
					System.err.println("departure: "+job_departure_time);
					System.err.println("arrival:    "+job.arrival_time);
					System.exit(1);
				}
				max_value = Math.max(max_value, job_sojourn);
				if (server.sampled_jobs.size() < 1000) {
					for (FJTask task : job.tasks) {
						writer.write(task.ID
								+"\t"+job.ID
								+"\t"+job.arrival_time
								+"\t"+job_start_time
								+"\t"+job_completion_time
								+"\t"+job_departure_time
								+"\t"+task.start_time
								+"\t"+task.completion_time
								+"\n");
					}
				}
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
	
	
	public static IntertimeProcess parseProcessSpec(String[] process_spec) {
		IntertimeProcess process = null;
		if (process_spec[0].equals("x")) {
			// exponential
			double rate = Double.parseDouble(process_spec[1]);
			process = new ExponentialIntertimeProcess(rate);
		} else if (process_spec[0].startsWith("e")) {
			// erlang k
			double rate = Double.parseDouble(process_spec[1]);
			int k = Integer.parseInt(process_spec[0].substring(1));
			process = new ErlangIntertimeProcess(rate, k);
		} else if (process_spec[0].equals("g") || process_spec[0].equals("n")) {
			// gaussian/normal
			double mean = Double.parseDouble(process_spec[1]);
			double var = Double.parseDouble(process_spec[2]);
			process = new FullNormalIntertimeProcess(mean, var);
		} else if (process_spec[0].equals("w")) {
			// weibull
			double shape = Double.parseDouble(process_spec[1]);
			if (process_spec.length == 2) {
				// normalized to have mean 1.0
				process = new WeibullIntertimeProcess(shape);
			} else if (process_spec.length == 3) {
				double scale = Double.parseDouble(process_spec[2]);
				process = new WeibullIntertimeProcess(shape, scale);
			}
		} else if (process_spec[0].equals("c")) {
			// constant inter-arrival times
			double rate = Double.parseDouble(process_spec[1]);
			process = new ConstantIntertimeProcess(rate);
		} else {
			System.err.println("ERROR: unable to parse process spec!");
			System.exit(1);
		}
		
		return process;
	}
	
	
	
	/**
	  * main()
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		//if (args == null || args.length != 8) {
		//	System.out.println("usage: FJSimulator <queue_type> <num_workers> <num_tasks> <arrival_rate> <service_rate> <numjobs> <sampling_interval> <filename_base>");
		//	System.exit(0);
		//}
		
		Options cli_options = new Options();
		cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("q", "queuetype", true, "queue type code");
		cli_options.addOption("w", "numworkers", true, "number of workers/servers");
		cli_options.addOption("t", "numtasks", true, "number of tasks per job");
		cli_options.addOption("n", "numjobs", true, "number of jobs to run");
		cli_options.addOption("i", "samplinginterval", true, "samplig interval");
		//cli_options.addOption("o", "outfile", true, "the base name of the output files");
		//cli_options.addOption(Option.builder("o").longOpt("outfile").hasArg().required().desc("the base name of the output files").build());
		//cli_options.addOption(Option.builder("A").longOpt("arrivalprocess").hasArgs().required().desc("arrival process").build());
		//cli_options.addOption(Option.builder("S").longOpt("serviceprocess").hasArgs().required().desc("service process").build());
		cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg().isRequired().withDescription("the base name of the output files").create("o"));
		cli_options.addOption(OptionBuilder.withLongOpt("arrivalprocess").hasArgs().isRequired().withDescription("arrival process").create("A"));
		cli_options.addOption(OptionBuilder.withLongOpt("serviceprocess").hasArgs().isRequired().withDescription("service process").create("S"));
		// TODO: add options for leaky bucket process filters
		
		//CommandLineParser parser = new DefaultParser();
		CommandLineParser parser = new PosixParser();
		CommandLine options = null;
		try {
			options = parser.parse(cli_options, args);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("FJSimulator", cli_options);
			e.printStackTrace();
			System.exit(0);
		}
		
		String server_queue_type = options.getOptionValue("q");
		int num_workers = Integer.parseInt(options.getOptionValue("w"));
		int num_tasks = Integer.parseInt(options.getOptionValue("t"));
		long num_jobs = Long.parseLong(options.getOptionValue("n"));
		int sampling_interval = Integer.parseInt(options.getOptionValue("i"));
		String outfile_base = options.getOptionValue("o");
		
		//
		// figure out the arrival process
		//
		String[] arrival_process_spec = options.getOptionValues("A");
		IntertimeProcess arrival_process = FJSimulator.parseProcessSpec(arrival_process_spec);
		
		//
		// figure out the service process
		//
		String[] service_process_spec = options.getOptionValues("S");
		IntertimeProcess service_process = FJSimulator.parseProcessSpec(service_process_spec);
		
		// data aggregator
		FJDataAggregator data_aggregator = new FJDataAggregator((int)(1 + num_jobs/sampling_interval));
		
		// simulator
		FJSimulator sim = new FJSimulator(server_queue_type, num_workers, num_tasks, arrival_process, service_process, data_aggregator);

		// start the simulator running...
		sim.run(num_jobs, sampling_interval);
		
		sim.printExperimentPath(outfile_base);
		
		data_aggregator.printExperimentDistributions(outfile_base, sim.binwidth);
		
		data_aggregator.printRawJobData(outfile_base);
		
		ArrayList<Double> means = data_aggregator.experimentMeans();
		System.out.println(
				num_workers
				+"\t"+num_tasks
				+"\t"+sim.server.num_stages
				+"\t"+sim.arrival_process.processParameters()
				+"\t"+sim.service_process.processParameters()
				+"\t"+means.get(0) // sojourn mean
				+"\t"+means.get(1) // waiting mean
				+"\t"+means.get(2) // service mean
				+"\t"+means.get(3) // total
				+"\t"+means.get(4) // sojourn quantile
				+"\t"+means.get(5) // waiting quantile
				+"\t"+means.get(6) // service quanile
				+"\t"+means.get(7) // sojourn quantile 2
				+"\t"+means.get(8) // waiting quantile 2
				+"\t"+means.get(9) // service quantile 2
				);
		
		//sim.jobAutocorrelation(outfile_base, 5000);
	}

}
