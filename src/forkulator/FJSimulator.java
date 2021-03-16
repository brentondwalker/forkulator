package forkulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.PriorityQueue;

import forkulator.randomprocess.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.HelpFormatter;
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
 * ./bin/spark-submit --master spark://172.23.27.10:7077  --executor-memory 40g --class forkulator.FJSparkSimulator /home/brenton/properbounds/forkulator-sbt/target/scala-2.10/forkulator-assembly-1.0.jar -q w -A x 0.5 -S x 1.0 -w 100 -t 100 -i 100 -n 1000000 -o testrun -s 500
 * 
 * @author brenton
 *
 */
public class FJSimulator {
	
	public static final boolean DEBUG = false;
	public static final int QUEUE_STABILITY_THRESHOLD = 100000;
	
	public PriorityQueue<QEvent> event_queue = new PriorityQueue<QEvent>();
	
	public int num_workers;
	public int num_tasks;
	public IntertimeProcess arrival_process;
	public IntertimeProcess service_process;
    public IntervalPartition job_partition_process = null;
    public IntervalPartition task_partition_process = null;
    public int task_division_factor = 0;
	public FJServer server = null;
	
	public double binwidth = 0.1;
	public double quanile_epsilon = 1e-6;
	
	public FJDataAggregator data_aggregator = null;
	
	
	/**
	 * constructor
	 * 
	 * @param server_queue_spec
	 * @param num_workers
	 * @param num_tasks
	 * @param arrival_process
	 * @param service_process
	 * @param data_aggregator
	 * @param path_logger
	 */
	public FJSimulator(String[] server_queue_spec,
			int num_workers, int num_tasks,
			IntertimeProcess arrival_process,
			IntertimeProcess service_process,
            IntervalPartition partition_process,
            int task_division_factor,
			FJDataAggregator data_aggregator) {
		this.num_workers = num_workers;
		this.num_tasks = num_tasks;
		this.arrival_process = arrival_process;
		this.service_process = service_process;
		this.data_aggregator = data_aggregator;
		if (task_division_factor > 0) {
		    this.task_partition_process = partition_process;
		    this.task_division_factor = task_division_factor;
		} else {
		    this.job_partition_process = partition_process;
		}
		
		String server_queue_type = server_queue_spec[0];
		
		if (server_queue_type.toLowerCase().equals("s")) {
			this.server = new FJSingleQueueServer(num_workers);
		} else if (server_queue_type.toLowerCase().equals("w")) {
			this.server = new FJWorkerQueueServer(num_workers); 
		} else if (server_queue_type.toLowerCase().equals("sm")) {
			this.server = new FJSingleQueueSplitMergeServer(num_workers);
        } else if (server_queue_type.toLowerCase().equals("hsm")) {
            this.server = new FJHalfwaySplitMergeServer(num_workers);
        } else if (server_queue_type.toLowerCase().equals("thsm")) {
            this.server = new FJTakeHalfSplitMergeServer(num_workers);
        } else if (server_queue_type.toLowerCase().equals("b") || server_queue_type.toLowerCase().equals("bb") || server_queue_type.toLowerCase().equals("nbb")) {
            if (num_workers < num_tasks) {
            	System.err.println("ERROR: FJBarrierServer requires num_workers >= num_tasks");
            	System.exit(0);
            }
            if (server_queue_type.toLowerCase().equals("bb")) {
            	// server with a departure barrier too
            	this.server = new FJBarrierServer(num_workers, true, true);
            } else if (server_queue_type.toLowerCase().equals("nbb")) {
            	// server with only departure barrier
            	this.server = new FJBarrierServer(num_workers, false, true);
            } else {
            	this.server = new FJBarrierServer(num_workers);
            }
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
		} else if (server_queue_type.toLowerCase().equals("wkl")) {
			if (server_queue_spec.length != 2) {
				System.err.println("ERROR: wkl/skl queue requires a numeric (k-l) parameter");
				System.exit(0);
			}
			int l_diff = Integer.parseInt(server_queue_spec[1]);
			this.server = new FJKLWorkerQueueServer(num_workers, num_workers - l_diff);
		} else if (server_queue_type.toLowerCase().equals("skl")) {
			if (server_queue_spec.length != 2) {
				System.err.println("ERROR: wkl/skl queue requires a numeric (k-l) parameter");
				System.exit(0);
			}
			int l_diff = Integer.parseInt(server_queue_spec[1]);
			this.server = new FJKLSingleQueueServer(num_workers, num_workers - l_diff);
		} else if (server_queue_type.toLowerCase().startsWith("msw")) {
			if (server_queue_spec.length != 2) {
				System.err.println("ERROR: msw/mswi queue requires a numeric num_stages parameter");
				System.exit(0);
			}

			// multi-stage worker-queue
			if (server_queue_type.toLowerCase().equals("mswi")) {
				// independent service times at each stage
				int num_stages = Integer.parseInt(server_queue_spec[1]);
				this.server = new FJMultiStageWorkerQueueServer(num_workers, num_stages, true);
			} else {
				// otherwise service times of each task stay the same across all stages
				int num_stages = Integer.parseInt(server_queue_spec[1]);
				this.server = new FJMultiStageWorkerQueueServer(num_workers, num_stages, false);
			}
		} else if (server_queue_type.toLowerCase().equals("sdl")) {
		    if (server_queue_spec.length != 2) {
                System.err.println("ERROR: sdl queue requires a numeric data_location_penalty parameter");
                System.exit(0);
            }
		    double data_location_penalty = Double.parseDouble(server_queue_spec[1]);
		    this.server = new FJSingleQueueDataLocationServer(num_workers, data_location_penalty);
		} else {
			System.err.println("ERROR: unknown server queue type: "+server_queue_type);
			System.exit(1);
		}
		this.server.setSimulator(this);
	}
	
	
	/**
	 * put the initial events in the simulation queue and start processing them
	 * 
	 * @param num_jobs
	 */
	public void run(long num_jobs, int sampling_interval) {
	    System.err.println("running a simulation for "+num_jobs+" jobs with samp interval "+sampling_interval);
		// compute the warmup period.
		// Let's say sampling_interval*10*num_stages
		int warmup_interval = sampling_interval * 10 * server.num_stages;
		
		// before we generated all the job arrivals at once
		// now to save space we only have one job arrival in the queue at a time
		event_queue.add(new QJobArrivalEvent(arrival_process.nextInterval()));
		// start processing events
		int sampling_countdown = sampling_interval;
		long jobs_processed = -warmup_interval;
		while (! event_queue.isEmpty()) {
			if (this.server.queueLength() > FJSimulator.QUEUE_STABILITY_THRESHOLD) {
				System.err.println("ERROR: queue exceeded threshold.  The system is unstable.");
				System.exit(0);
			}
			QEvent e = event_queue.poll();

			if (e instanceof QJobArrivalEvent) {
				jobs_processed++;
				if (((jobs_processed*100)%num_jobs)==0)
					System.err.println("   ... "+(100*jobs_processed/num_jobs)+"%\tqueuesize="+server.queueLength());
				QJobArrivalEvent et = (QJobArrivalEvent) e;

				FJJob job;
				if (this.job_partition_process != null) {
					job = new FJRandomPartitionJob(num_tasks, server.num_workers, service_process, job_partition_process, e.time);
				} else if (this.task_partition_process != null) {
				    job = new FJRandomTaskPartitionJob(num_tasks, server.num_workers, task_division_factor, service_process, task_partition_process, e.time);
				} else {
					job = new FJIndependentTaskJob(num_tasks, server.num_workers, service_process, e.time);
				}
				job.arrival_time = et.time;
				if (jobs_processed >= 0) {
					if (data_aggregator.path_logger != null) {
						data_aggregator.path_logger.addJob(job);
					}
					if (sampling_countdown==0) {
						server.enqueJob(job, true);
						sampling_countdown = sampling_interval;
					} else {
						server.enqueJob(job, false);
					}
					sampling_countdown--;
				}


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
	    event_queue.add(e);
	}
	
	
	/**
	 * compute the means of sojourn, waiting, and service times over (almost) all jobs
	 * 
	 * @param warmup_period
	 * @return
	 */
	public ArrayList<Double> experimentMeans() {
		double sojourn_sum = 0.0;
		double waiting_sum = 0.0;
		double service_sum = 0.0;
		for (FJJob job : server.sampled_jobs) {
			double job_start_time = job.tasks[0].start_time;
			double job_completion_time = job.tasks[0].completion_time;
			for (FJTask task : job.tasks) {
				job_start_time = Math.min(job_start_time, task.start_time);
				job_completion_time = Math.max(job_completion_time, task.completion_time);
			}
			waiting_sum += job_start_time - job.arrival_time;
			sojourn_sum += job_completion_time - job.arrival_time;
			service_sum += job_completion_time - job_start_time;
		}
		
		double total = server.sampled_jobs.size();
		ArrayList<Double> result = new ArrayList<Double>(3 + 1);
		result.add(sojourn_sum/total);
		result.add(waiting_sum/total);
		result.add(service_sum/total);
		result.add(total * 1.0);
		return result;
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
	 * Parse the arguments given for either the arrival or service process, and
	 * return an appropriately configured IntertimeProcess.
	 * 
	 * @param process_spec
	 * @return
	 */
	public static IntertimeProcess parseProcessSpec(String[] process_spec) {
		IntertimeProcess process = null;
		if (process_spec[0].equals("x")) {
			// exponential
			double rate = Double.parseDouble(process_spec[1]);
			process = new ExponentialIntertimeProcess(rate);
		} else if (process_spec[0].equals("e")) {
			// erlang k
			int k = Integer.parseInt(process_spec[1]);
			double rate = Double.parseDouble(process_spec[2]);
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
		} else if (process_spec[0].equals("xr")) {
			// correlated exponential
			// requires three args: rate, batch_size, rho
			System.out.println(Arrays.toString(process_spec));
			double rate = Double.parseDouble(process_spec[1]);
			int k = Integer.parseInt(process_spec[2]);
			double rho = Double.parseDouble(process_spec[3]);
			process = new CorrelatedExponentialIntertimeProcess(rate, k, rho);
		} else if (process_spec[0].equals("xrs")) {
		    // sum of correlated exponentials
		    // meant to be used with -J option
		    // requires three args: rate, batch_size, rho
		    System.out.println(Arrays.toString(process_spec));
		    double rate = Double.parseDouble(process_spec[1]);
		    int k = Integer.parseInt(process_spec[2]);
		    double rho = Double.parseDouble(process_spec[3]);
		    process = new CorrelatedExponentialSumIntertimeProcess(rate, k, rho);
		} else {
			System.err.println("ERROR: unable to parse process spec!");
			System.exit(1);
		}
		
		return process;
	}
	
	/**
     * Parse the arguments given for either the arrival or service process, and
     * return an appropriately configured IntertimeProcess.
     * 
     * @param process_spec
     * @return
     */
    public static IntervalPartition parseJobDivisionSpec(String[] process_spec) {
        IntervalPartition process = null;
        if (process_spec[0].equals("u")) {
            // uniform
            process = new UniformRandomIntervalPartition(1.0, 1, false);
        } else if (process_spec[0].equals("ui")) {
            // uniform with independent samples
            process = new UniformRandomIntervalPartition(1.0, 1, true);
        } else if (process_spec[0].equals("c")) {
			process = new ConstantIntervalPartition(1.0, 1);
		} else if (process_spec[0].equals("m")) {
			process = new MultinomialIntervalPartition(1.0, 1);
		} else if (process_spec[0].equals("m2")) {
			process = new MultinomialIntervalPartitionSingleWell(1.0, 1);
		} else if (process_spec[0].equals("x")) {
			if (process_spec.length < 2) {
				System.err.println("ERROR: Not enough arguments for exponential partitioning!");
				System.exit(1);
			}
			process = new ExponentialRandomIntervalPartition(1.0, 1,
					Double.parseDouble(process_spec[1]));
		} else if (process_spec[0].equals("t")) {
        	if (process_spec.length < 4) {
				System.err.println("ERROR: Not enough arguments for twice split partition!");
				System.exit(1);
			}
			int partDiv = Integer.parseInt(process_spec[1]);
			IntervalPartition first =
					FJSimulator.parseJobDivisionSpec(Arrays.copyOfRange(process_spec, 2,
							process_spec.length));
			IntervalPartition second = null;
			if(process_spec[2].equals("x")) {
				second =
						FJSimulator.parseJobDivisionSpec(Arrays.copyOfRange(process_spec, 4,
								process_spec.length));
			} else {
				second =
						FJSimulator.parseJobDivisionSpec(Arrays.copyOfRange(process_spec, 3,
								process_spec.length));
			}
			process = new TwiceSplitIntervalPartition(partDiv, first, second);
		}  else {
            System.err.println("ERROR: unable to parse job division spec!" + Arrays.toString(process_spec));
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
		
		Options cli_options = new Options();
		cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("w", "numworkers", true, "number of workers/servers");
		cli_options.addOption("t", "numtasks", true, "number of tasks per job");
		cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run");
		cli_options.addOption("i", "samplinginterval", true, "samplig interval");
		cli_options.addOption("p", "savepath", true, "save some iterations of the simulation path (arrival time, service time etc...)");
		cli_options.addOption(OptionBuilder.withLongOpt("queuetype").hasArgs().isRequired().withDescription("queue type and arguments").create("q"));
		cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg().isRequired().withDescription("the base name of the output files").create("o"));
		cli_options.addOption(OptionBuilder.withLongOpt("arrivalprocess").hasArgs().isRequired().withDescription("arrival process").create("A"));
		cli_options.addOption(OptionBuilder.withLongOpt("serviceprocess").hasArgs().isRequired().withDescription("service process").create("S"));
        cli_options.addOption(OptionBuilder.withLongOpt("jobpartition").hasArgs().withDescription("job_partition").create("J"));
        cli_options.addOption(OptionBuilder.withLongOpt("taskpartition").hasArgs().withDescription("task_partition").create("T"));

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
		
		String[] server_queue_spec = options.getOptionValues("q");
		int num_workers = Integer.parseInt(options.getOptionValue("w"));
		int num_tasks = Integer.parseInt(options.getOptionValue("t"));
		long num_samples = Long.parseLong(options.getOptionValue("n"));
		int sampling_interval = Integer.parseInt(options.getOptionValue("i"));
		String outfile_base = options.getOptionValue("o");

		// compute the number of jobs necessary to get the desired samples
		long num_jobs = num_samples * sampling_interval;
		
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
		
		//
        // if we are in job-partitioning mode, figure out the partitioning type
        //
        IntervalPartition partition_process = null;
        int task_division_factor = 1;
        if (options.hasOption("J")) {
            if (options.hasOption("T")) {
                System.err.println("ERROR: cannot use both the -J and -T options together");
                System.exit(0);
            }
            String[] job_partition_spec = options.getOptionValues("J");
            partition_process = FJSimulator.parseJobDivisionSpec(job_partition_spec);
        }
        
        //
        // Task partitioning mode is like job partitioning, but there can be many
        // tasks that are divided into smaller tasks.  Strictly speaking, job-partitioning
        // mode is a special case of task partitioning mode (take tasks=1), but for now we
        // configure them differently.
        //
        if (options.hasOption("T")) {
            String[] task_partition_spec = options.getOptionValues("T");
            // for task division, the arguments are the same as job division, except the
            // first argument is the number of subtasks to divide each task into.
            task_division_factor = Integer.parseInt(task_partition_spec[0]);
            partition_process = FJSimulator.parseJobDivisionSpec(Arrays.copyOfRange(task_partition_spec, 1, task_partition_spec.length));
        }
		
		// data aggregator
		FJDataAggregator data_aggregator = new FJDataAggregator((int)(1 + num_jobs/sampling_interval));
		
		// optional path logger
		if (options.hasOption("p")) {
			data_aggregator.path_logger = new FJPathLogger((int)(1 + num_jobs/sampling_interval), num_tasks);
		}
		
		// simulator
		FJSimulator sim = new FJSimulator(server_queue_spec, num_workers, num_tasks, arrival_process, service_process, partition_process, task_division_factor, data_aggregator);

		// start the simulator running...
		sim.run(num_jobs, sampling_interval);

		if (sim.data_aggregator.path_logger != null) {
			sim.data_aggregator.path_logger.writePathlog(outfile_base, false);
		}

		data_aggregator.printExperimentDistributions(outfile_base, sim.binwidth);

		data_aggregator.printRawJobData(outfile_base);

		ArrayList<Double> means = data_aggregator.experimentMeans();
		System.out.println(
				num_workers                                     // 1
				+"\t"+num_tasks                                 // 2
				+"\t"+sim.server.num_stages                     // 3
				+"\t"+sim.arrival_process.processParameters()   // 4  // this can be more than one value
				+"\t"+sim.service_process.processParameters()   // 5  // this can be more than one value
				+"\t"+means.get(0) // sojourn mean                 6
				+"\t"+means.get(1) // waiting mean                 7
				+"\t"+means.get(2) // lasttask mean                8				
				+"\t"+means.get(3) // service mean                 9
				+"\t"+means.get(4) // cpu mean                     10
				+"\t"+means.get(5) // total                        11
                +"\t"+means.get(6) // job sojourn 1e-6 quantile    12
                +"\t"+means.get(7) // job waiting 1e-6 quantile    13
                +"\t"+means.get(8) // job lasttask 1e-6 quantile   14
                +"\t"+means.get(9) // job service 1e-6 quantile    15
                +"\t"+means.get(10) // job cputime 1e-6 quantile   16
                +"\t"+means.get(11) // job sojourn 1e-3 quantile   17
                +"\t"+means.get(12) // job waiting 1e-3 quantile   18
                +"\t"+means.get(13) // job lasttask 1e-3 quantile  19
                +"\t"+means.get(14) // job service 1e-3 quantile   20
                +"\t"+means.get(15) // job cputime 1e-3 quantile   21
				);

//		sim.jobAutocorrelation(outfile_base, 5000);
	}

}
