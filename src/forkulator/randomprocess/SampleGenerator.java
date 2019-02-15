package forkulator.randomprocess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import forkulator.FJSimulator;

/**
 * This is a tool to generate samples from the various distributions in forkulator.randomprocess.
 * 
 * @author brenton
 *
 */
public class SampleGenerator {
    
    
    /**
     * Constructor
     * 
     * The interval is [0,size], and it will be divided by (num_partitions-1)
     * boundaries placed uniformly randomly.
     * 
     * @param size
     * @param num_partitions
     */
    public SampleGenerator(double size, int num_partitions) {
        
        
    }    
    
    /**
     * Include a main() routine to produce samples from this type of distribution.
     * 
     * @param args
     */
    public static void main(String[] args) {
        Options cli_options = new Options();
        cli_options.addOption("h", "help", false, "print help message");
        cli_options.addOption("t", "numtasks", true, "number of tasks per job");
        cli_options.addOption("w", "numworkers", true, "number of workers/servers.  This will multiply the job service times.");
        cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run");
        cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg().isRequired().withDescription("output file to store samples").create("o"));
        cli_options.addOption(OptionBuilder.withLongOpt("serviceprocess").hasArgs().isRequired().withDescription("service process").create("S"));
        cli_options.addOption(OptionBuilder.withLongOpt("jobpartition").hasArgs().withDescription("job_partition").create("J"));
        
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
        
        int num_tasks = Integer.parseInt(options.getOptionValue("t"));
        int num_workers = Integer.parseInt(options.getOptionValue("w"));
        long num_samples = Long.parseLong(options.getOptionValue("n"));
        String outfile = options.getOptionValue("o");

        //
        // figure out the service process
        //
        String[] service_process_spec = options.getOptionValues("S");
        IntertimeProcess service_process = FJSimulator.parseProcessSpec(service_process_spec);
        
        //
        // if we are in job-partitioning mode, figure out the partitioning type
        //
        boolean job_partition_mode = false;
        IntervalPartition job_partition_process = null;
        if (options.hasOption("J")) {
            String[] job_partition_spec = options.getOptionValues("J");
            job_partition_mode = true;
            job_partition_process = FJSimulator.parseJobDivisionSpec(job_partition_spec);
        }

        BufferedWriter out = null;
        try {
            FileWriter fstream = new FileWriter(outfile, false);
            out = new BufferedWriter(fstream);        }
        catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(0);
        }

        // in job_partiton_mode, the jobs have a service time drawn from the service_process,
        // and then the tasks are sub-divisions of that.
        if (job_partition_mode) {
            IntertimeProcess p = null;
            int batch_num = -1;
            for (int i=0; i<num_samples; i++) {
                if ((i % num_tasks) == 0) {
                    p = job_partition_process.getNewPartiton(service_process.nextInterval()*num_workers, num_tasks);
                    batch_num++;
                }
                try {
                    out.write(String.join("\t", new String[] { Integer.toString(i), Integer.toString(batch_num), Integer.toString(i % num_tasks), Double.toString(p.nextInterval()) })+"\n");
                }
                catch (IOException e) {
                    System.err.println("Error: " + e.getMessage());
                    System.exit(0);
                }
            }
            
        } else {
            // in normal mode the task service times are drawn from the service_process
            int batch_num = -1;
            for (int i=0; i<num_samples; i++) {
                if ((i % num_tasks) == 0) {
                    batch_num++;
                }
                try {
                    out.write(String.join("\t", new String[] { Integer.toString(i), Integer.toString(batch_num), Integer.toString(i % num_tasks), Double.toString(service_process.nextInterval()) })+"\n");
                }
                catch (IOException e) {
                    System.err.println("Error: " + e.getMessage());
                    System.exit(0);
                }
            }
        }
        
        if(out != null) {
            try {
                out.close();
            } catch (IOException e) {
                System.err.println("Error: " + e.getMessage());
                System.exit(0);
            }
        }
    }

    public static void usage() {
        System.out.println("usage: UniformRandomIntervalPartition <k> <num_samples> <outfile>\n");
        System.exit(0);
    }
    
}
