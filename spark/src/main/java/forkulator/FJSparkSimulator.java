package forkulator;

import org.apache.spark.api.java.JavaSparkContext;

import forkulator.randomprocess.IntertimeProcess;

import org.apache.spark.api.java.JavaRDD;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.SparkConf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;


/**
 * Simulate a Fork-Join system on a spark cluster!
 * 
 * ./bin/spark-submit --master spark://172.23.27.20:7077  --conf spark.cores.max=2 --class forkulator.FJSparkSimulator /home/brenton/properbounds/forkulator-sbt/target/scala-2.10/forkulator-assembly-1.0.jar -q w -A x 0.5 -S x 1.0 -w 10 -t 10 -i 1 -n 1000000 -o testrun
 * 
 * ./bin/spark-submit --master spark://172.23.27.10:7077  --executor-memory 40g --class forkulator.FJSparkSimulator /home/brenton/properbounds/forkulator-sbt/target/scala-2.10/forkulator-assembly-1.0.jar -q w -A x 0.5 -S x 1.0 -w 10 -t 10 -i 10 -n 10000000 -o testrun -s 50
 * 
 * @author brenton
 *
 */
public class FJSparkSimulator {
	
	/**
	 * This is the method that runs on the executors.  It should run one
	 * slice of the simulation.  It is kind-of awkward that this method
	 * computes how big a slice should be, but it's easier that way because
	 * I just pass in the options object.
	 * 
	 * @param options
	 * @param s
	 * @return
	 */
    public static FJDataAggregator doSimulation(CommandLine options, int segment_index) {
		String server_queue_type = options.getOptionValue("q");
		int num_workers = Integer.parseInt(options.getOptionValue("w"));
		int num_tasks = Integer.parseInt(options.getOptionValue("t"));
		long num_samples = Long.parseLong(options.getOptionValue("n"));
		int num_slices = 1;
		if (options.hasOption("s")) { num_slices = Integer.parseInt(options.getOptionValue("s")); }
		int sampling_interval = Integer.parseInt(options.getOptionValue("i"));
		String outfile_base = options.getOptionValue("o");
		
		// compute how many samples, and how many jobs are needed from each slice (round up)
		int samples_per_slice = (int) Math.ceil( ((double)num_samples) / num_slices );
		long jobs_per_slice = ((long) samples_per_slice) * ((long) sampling_interval);

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
		FJDataAggregator data_aggregator = new FJDataAggregator(samples_per_slice);

		// optional path logger
		// when running on Spark we only do the path logging for the first slice
		if ((segment_index==0) && (options.hasOption("p"))) {
		    data_aggregator.path_logger = new FJPathLogger(Integer.parseInt(options.getOptionValue("p")), num_tasks);
		}
		
		// simulator
		String[] server_queue_spec = options.getOptionValues("q");
		FJSimulator sim = new FJSimulator(server_queue_spec, num_workers, num_tasks, arrival_process, service_process, data_aggregator);

		// start the simulator running...
		sim.run(jobs_per_slice, sampling_interval);
		
		return sim.data_aggregator;
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("forkulator");  //.setMaster(master);
		JavaSparkContext spark = new JavaSparkContext(conf);
		
		Options cli_options = new Options();
		cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("w", "numworkers", true, "number of workers/servers");
		cli_options.addOption("t", "numtasks", true, "number of tasks per job");
		cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run");
		cli_options.addOption("i", "samplinginterval", true, "samplig interval");
		cli_options.addOption("p", "savepath", true, "save some iterations of the simulation path (arrival time, service time etc...)");
		cli_options.addOption("s", "numslices", true, "the number of slices to divide te job into.  This is ideally a multiple of the number of cores.");
		cli_options.addOption(OptionBuilder.withLongOpt("queuetype").hasArgs().isRequired().withDescription("queue type and arguments").create("q"));
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
		System.out.println("options: "+options);
		
		// we need to make the options final in order to use them in a lambda expression
		final CommandLine foptions = options;
		
		// how many slices to divide the simulation into
		// This doesn't have to equal the number of cores.  If there
		// are more slices than cores they will just execute in tandem.
		// But it is most efficient if the number of slices is a multiple
		// of the number of cores.
		//XXX - It would be nicer to divide this into segments equal to the number of
		//      cores and write out data as we go.
		int num_slices = 1;
		if (options.hasOption("s")) { num_slices = Integer.parseInt(options.getOptionValue("s")); }
		
		// distribute the simulation segments to workers
		ArrayList<Integer> ar = new ArrayList<Integer>(num_slices);
		for (int i=0; i<num_slices; i++) { ar.add(i); }
		JavaRDD<FJDataAggregator> rdd = spark.parallelize(ar, num_slices).map(s -> doSimulation(foptions,s)).cache();
		List<FJDataAggregator> dl = rdd.collect();
		//System.out.println("rdd = "+rdd);
		//System.out.println("dl = "+dl);
		
		// write out the path data, if it was recorded
		String outfile_base = options.getOptionValue("o");
		if (dl.get(0).path_logger != null) {
		    dl.get(0).path_logger.writePathlog(outfile_base, false);
		}
		
		BufferedWriter writer = null;
		try {
			GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(outfile_base+"_jobdat.dat.gz")));
			writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
			//final BufferedWriter fwriter = writer;
			//rdd.foreach(d -> d.appendRawJobData(fwriter));
			for (FJDataAggregator d : dl) {
				d.appendRawJobData(writer);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				// Close the writer regardless of what happens...
				writer.close();
			} catch (Exception e) {
			}
		}
		
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
