package forkulator.randomprocess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.math3.stat.StatUtils;

import forkulator.FJSimulator;


/**
 * In a Split-Merge system, unless the workload on each server is identical,
 * some servers will sit idle while waiting for the largest task(s) to finish.
 * This has nothing to do with the queueing or utilization, so we can compute
 * statistics about idle time without running a full simulation.  We just
 * generate a bunch of sample jobs with the desired distributions.
 * 
 * @author brenton
 *
 */
public class IdletimeAnalysis {

    /**
     * Include a main() routine to produce samples from this type of distribution.
     * 
     * @param args
     */
    public static void main(String[] args) {
        Options cli_options = new Options();
        cli_options.addOption("h", "help", false, "print help message");
        cli_options.addOption("t", "numtasks", true, "In Job-Partition mode, the number of tasks per job");
        cli_options.addOption("w", "numworkers", true, "In Job-Partition mode this will multiply the job service times.");
        cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run");
        cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg().withDescription("output file to store samples").create("o"));
        cli_options.addOption(OptionBuilder.withLongOpt("serviceprocess").hasArgs().isRequired().withDescription("service process").create("S"));
        cli_options.addOption(OptionBuilder.withLongOpt("jobpartition").hasArgs().withDescription("job_partition").create("J"));
        
        //CommandLineParser parser = new DefaultParser();
        CommandLineParser parser = new PosixParser();
        CommandLine options = null;
        try {
            options = parser.parse(cli_options, args);
        } catch (ParseException e) {
            System.out.println("\nERROR: "+e+"\n");
            usage(cli_options);
        }
        
        if (options.hasOption("h")) {
            usage(cli_options);
            System.exit(0);
        }
        
        int num_tasks = Integer.parseInt(options.getOptionValue("t"));
        int num_workers = Integer.parseInt(options.getOptionValue("w"));
                
        int num_samples = 100;
        if (options.hasOption("n")) {
            num_samples = Integer.parseInt(options.getOptionValue("n"));
        }
        
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
            Writer fstream;
            if (outfile != null) {
                fstream = new FileWriter(outfile, false);
            } else {
                fstream = new OutputStreamWriter(System.out);
            }
            out = new BufferedWriter(fstream);        }
        catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(0);
        }

        // array to hold the samples
        double samples[] = new double[num_samples];
        double sample_sum = 0.0;
        
        for (int i=0; i<num_samples; i++) {
            double workloads[] = new double[num_workers];

            // append each task to the smallest workload.
            // brute force - nothing fancy about this.
            for (int j=0; j<num_tasks; j++) {
                
                // find the current smallest workload
                int min_j = 0;
                double min_workload = Double.POSITIVE_INFINITY;
                for (int jx=0; jx<num_workers; jx++) {
                    if (workloads[jx] == 0.0) {
                        min_j = jx;
                        break;
                    } else if (workloads[jx] < min_workload) {
                        min_workload = workloads[jx];
                        min_j = jx;
                    }
                }
                
                // we can treat job_partition mode and normal mode the same
                double s = service_process.nextInterval();
                workloads[min_j] += s;
            }
            
            // find the largest workload
            int max_j = 0;
            double max_workload = 0.0;
            for (int jx=0; jx<num_workers; jx++) {
                if (workloads[jx] == 0.0) {
                    max_j = jx;
                    break;
                } else if (workloads[jx] > max_workload) {
                    max_workload = workloads[jx];
                    max_j = jx;
                }
            }
            
            // sample just one task per job, as the other idle times may be correlated
            samples[i] = max_workload - workloads[0];
            /*
            // exclude the largest workload which has idle time = 0?
            if (max_j != 0) {
                samples[i] = max_workload - workloads[0];
            } else {
                samples[i] = max_workload - workloads[1];
            }
            */
            sample_sum += samples[i];
            
            try {
                out.write(String.join("\t", new String[] { Integer.toString(i), Double.toString(max_workload), Double.toString(samples[i]) })+"\n");
            }
            catch (IOException e) {
                System.err.println("Error: " + e.getMessage());
                System.exit(0);
            }
        }
        
        double sample_mean = StatUtils.mean(samples);
        double sample_var = StatUtils.variance(samples, sample_mean);
        double sample_q3 = StatUtils.percentile(samples, 1.0e-3);
        double sample_q6 = StatUtils.percentile(samples, 1.0e-6);
        
        System.out.println(""+num_workers+"\t"+num_tasks+"\t"+num_samples+"\t"+sample_mean+"\t"+sample_var+"\t"+sample_q3+"\t"+sample_q6);
        
        if(out != null) {
            try {
                out.close();
            } catch (IOException e) {
                System.err.println("Error: " + e.getMessage());
                System.exit(0);
            }
        }
    }

    public static void usage(Options cli_options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SampleGenerator", cli_options);
        System.exit(0);
    }
    
}

