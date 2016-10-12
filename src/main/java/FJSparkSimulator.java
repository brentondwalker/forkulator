package forkulator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.SparkConf;


public class FJSparkSimulator {
	
	
	public static int doSimulation(CommandLine options, int s) {
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
		
		return 1;
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("forkulator");  //.setMaster(master);
		JavaSparkContext spark = new JavaSparkContext(conf);
		
		Options cli_options = new Options();
		cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("q", "queuetype", true, "queue type code");
		cli_options.addOption("w", "numworkers", true, "number of workers/servers");
		cli_options.addOption("t", "numtasks", true, "number of tasks per job");
		cli_options.addOption("n", "numjobs", true, "number of jobs to run");
		cli_options.addOption("i", "samplinginterval", true, "samplig interval");
		cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg().isRequired().withDescription("the base name of the output files").create("o"));
		cli_options.addOption(OptionBuilder.withLongOpt("arrivalprocess").hasArgs().isRequired().withDescription("arrival process").create("A"));
		cli_options.addOption(OptionBuilder.withLongOpt("serviceprocess").hasArgs().isRequired().withDescription("service process").create("S"));
		
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
		
		System.out.println("HEY!!!");
		
		final CommandLine foptions = options;
		spark.parallelize(Arrays.asList(1,2),2)
		  .map(s -> doSimulation(foptions,s))
		  .count();
		
		try {
			Thread.sleep(1000);
		} catch (java.lang.InterruptedException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("FJSimulator", cli_options);
			e.printStackTrace();
		}
		spark.stop();
	}
	
}
