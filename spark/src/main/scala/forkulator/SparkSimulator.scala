package forkulator

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, LogManager, Logger, PropertyConfigurator}
import java.util
import java.util.ArrayList

import forkulator.randomprocess.{IntertimeProcess, IntervalPartition}
import org.apache.commons.cli.{CommandLine, CommandLineParser, HelpFormatter, OptionBuilder, Options, ParseException, PosixParser}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.io.File
import org.apache.hadoop.conf.Configuration



object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
class SparkSimulator {


}

object SparkSimulator {
  val metricType = StructType(
    Seq(
      StructField(name = "sliceNum", dataType = LongType, nullable = false),
      StructField(name = "sampleNum", dataType = IntegerType, nullable = false),
      StructField(name = "waitingTime", dataType = DoubleType, nullable = false),
      StructField(name = "sojournTime", dataType = DoubleType, nullable = false),
      StructField(name = "serviceTime", dataType = DoubleType, nullable = false),
      StructField(name = "cpuTime", dataType = DoubleType, nullable = false),
      StructField(name = "maxSojournTimeIncreasing", dataType = IntegerType, nullable = false),
      StructField(name = "param", dataType = StringType, nullable = false)
    )
  )

  def getCliOptions: Options = {
    val cli_options = new Options
    cli_options.addOption("h", "help", false, "print help message")
    cli_options.addOption("w", "numworkers", true, "number of workers/servers")
    cli_options.addOption("t", "numtasks", true, "number of tasks per job")
    cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run")
    cli_options.addOption("i", "samplinginterval", true, "samplig interval")
    cli_options.addOption("p", "savepath", true, "save some iterations of the simulation path (arrival time, service time etc...)")
    cli_options.addOption("s", "numslices", true, "the number of slices to divide te job into.  This is ideally a multiple of the number of cores.")
    OptionBuilder.hasArgs
    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("queuetype")
    OptionBuilder.withDescription("queue type and arguments")
    cli_options.addOption(OptionBuilder.create("q"))
    //    cli_options.addOption(OptionBuilder.withLongOpt("queuetype").hasArgs().isRequired
    //      .withDescription("queue type and arguments").create("q"))
    OptionBuilder.hasArgs
    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("outfile")
    OptionBuilder.withDescription("the base name of the output files")
    cli_options.addOption(OptionBuilder.create("o"))
    //    cli_options.addOption(OptionBuilder.withLongOpt("outfile").hasArg.isRequired.withDescription("the base name of the output files").create("o"))
    OptionBuilder.hasArgs
    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("arrivalprocess")
    OptionBuilder.withDescription("arrival process")
    cli_options.addOption(OptionBuilder.create("A"))
    //    cli_options.addOption(OptionBuilder.withLongOpt("arrivalprocess").hasArgs.isRequired.withDescription("arrival process").create("A"))
    OptionBuilder.hasArgs
    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("serviceprocess")
    OptionBuilder.withDescription("service process")
    cli_options.addOption(OptionBuilder.create("S"))
    //    cli_options.addOption(OptionBuilder.withLongOpt("serviceprocess").hasArgs.isRequired.withDescription("service process").create("S"))
    OptionBuilder.hasArgs
    //    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("jobpartition")
    OptionBuilder.withDescription("job_partition")
    cli_options.addOption(OptionBuilder.create("J"))
    OptionBuilder.hasArgs
    OptionBuilder.withLongOpt("check_stability")
    OptionBuilder.withDescription("check_stability")
    cli_options.addOption(OptionBuilder.create("cs"))
    cli_options
  }

  def doSimulation(options: CommandLine, segment_index: Int, aggregator: FJBaseDataAggregator,
                   arrivalSpec: Array[String] = Array.empty, serviceValues: Array[String] = Array.empty)
  : FJBaseDataAggregator = {
//    try
//      options = Some(parser.parse(SparkSimulator.getCliOptions, args))
//    catch {
//      case e: ParseException =>
//        val formatter = new HelpFormatter
//        formatter.printHelp("FJSimulator", SparkSimulator.getCliOptions)
//        e.printStackTrace()
//        System.exit(0)
//    }


    val server_queue_type = options.getOptionValue("q")
    val num_workers = options.getOptionValue("w").toInt
    val num_tasks = options.getOptionValue("t").toInt
    val num_samples = (options.getOptionValue("n")).toLong
    var num_slices = 1
    if (options.hasOption("s")) num_slices = options.getOptionValue("s").toInt
    val sampling_interval = options.getOptionValue("i").toInt
    val outfile_base = options.getOptionValue("o")
    // compute how many samples, and how many jobs are needed from each slice (round up)
    val samples_per_slice = Math.ceil(num_samples.toDouble / num_slices).toInt
    val jobs_per_slice = samples_per_slice.toLong * sampling_interval.toLong
    //
    // figure out the arrival process
//    val arrival_process_spec = a
//    if (arrival_process_spec.isEmpty)
//      arrival_process_spec = options.getOptionValues("A")
    val arrival_process = FJSimulator.parseProcessSpec(if (arrivalSpec.isEmpty) options
      .getOptionValues("A") else arrivalSpec)
    // figure out the service process
//    val service_process_spec = options.getOptionValues("S")
    val service_process = FJSimulator.parseProcessSpec(if (serviceValues.isEmpty) options
      .getOptionValues("S") else serviceValues)
    // if we are in job-partitioning mode, figure out the partitioning type
    var job_partition_process: Option[IntervalPartition] = None
    if (options.hasOption("J")) {
      val job_partition_spec = options.getOptionValues("J")
      job_partition_process = Some(FJSimulator.parseJobDivisionSpec(job_partition_spec))
    }
    // data aggregator
    val data_aggregator: FJBaseDataAggregator = aggregator.getNewInstance(samples_per_slice,
      aggregator.batch_size)
    // optional path logger
    // when running on Spark we only do the path logging for the first slice
    if ((segment_index == 0) && options.hasOption("p")) data_aggregator.path_logger = new FJPathLogger(options.getOptionValue("p").toInt, num_tasks)
    // simulator
    val server_queue_spec = options.getOptionValues("q")


    val sim = new FJSimulator(server_queue_spec, num_workers, num_tasks, arrival_process,
      service_process, job_partition_process.getOrElse(null), data_aggregator)
    // start the simulator running...
    sim.run(jobs_per_slice, sampling_interval)
    sim.data_aggregator
  }

//  def doSimulation(simulation: FJSimulator) = {
//    simulation.run(jobs_per_slice, sampling_interval)
////    sim.data_aggregator
//  }

  def main(args: Array[String]): Unit = {
//    val log = LogManager.getRootLogger
//    args.map(a => log.warn(a))
//    args.map(a => println(a))
    args.foreach(a => println(a))
    val conf = new SparkConf().setAppName("forkulator") //.setMaster(master);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array[Class[_]](classOf[FJPathLogger], classOf[FJDataAggregator]))
    val spark = new sql.SparkSession.Builder().appName("forkulator").config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //    cli_options.addOption(OptionBuilder.withLongOpt("jobpartition").hasArgs.withDescription("job_partition").create("J"))
    val parser = new PosixParser
    var options: Option[CommandLine] = None
    try
      options = Some(parser.parse(SparkSimulator.getCliOptions, args))
    catch {
      case e: ParseException =>
        val formatter = new HelpFormatter
        formatter.printHelp("FJSimulator", SparkSimulator.getCliOptions)
        e.printStackTrace()
        System.exit(0)
    }
//    System.out.println("options: " + options)
    // we need to make the options final in order to use them in a lambda expression
//    val foptions = options
    // how many slices to divide the simulation into
    // This doesn't have to equal the number of cores.  If there
    // are more slices than cores they will just execute in tandem.
    // But it is most efficient if the number of slices is a multiple
    // of the number of cores.
    //XXX - It would be nicer to divide this into segments equal to the number of
    //      cores and write out data as we go.
    var num_slices = 1
    var checkStability = ""
    var stabilityRegion = 0.0
    var stabilityResolution = 0.5
    if (options.exists(_.hasOption("s"))) num_slices = options.get.getOptionValue("s")
      .toInt
    if (options.exists(_.hasOption("cs"))) {
      val stabilityOptions = options.get.getOptionValues("cs")
      checkStability = stabilityOptions(0)
      stabilityRegion = stabilityOptions(1).toDouble
      stabilityResolution = stabilityOptions(2).toDouble
    }
    val outfile_base = options.map(_.getOptionValue("o")).getOrElse("out")
    // distribute the simulation segments to workers
    val ar = new util.ArrayList[Integer](num_slices)
//    var i = 0
//    while ( {
//      i < num_slices
//    }) {
//      ar.add(i)
//
//      {
//        i += 1; i - 1
//      }
//    }
//    println(args)
    if (checkStability.equals("")) {
      val rdd = spark.createDataFrame(sc.parallelize(0 until num_slices, num_slices)
        .map(s => SparkSimulator.doSimulation(options.get, s, new FJDataAggregator(num_slices)))
        .zipWithIndex().flatMap{case (f, sliceIdx) => (0 until f.max_samples).map(i => {
        f match {
          case f: FJDataAggregator => Row(
            sliceIdx,
            i,
            f.job_start_time(i) - f.job_arrival_time(i),
            f.job_departure_time(i) - f.job_arrival_time(i),
            f.job_completion_time(i) - f.job_start_time(i),
            f.job_cpu_time(i)
          )
//          case _ => Row()
        }
      })
      }, metricType)//.toDF().cache
      rdd.write.parquet(outfile_base)
    } else {
      println(checkStability)
      val arrivalSpec = options.get.getOptionValues("A")
      val serviceSpec = options.get.getOptionValues("S")
      var parametersToChange = Array.empty[String]
      checkStability match {
        case "S" => parametersToChange = serviceSpec
        case "A" => parametersToChange = arrivalSpec
      }
      var currentResolution = if (stabilityResolution > 0.1) stabilityResolution else 0.1
      var minStability = parametersToChange(1).toDouble - stabilityRegion
      var maxStability = parametersToChange(1).toDouble + stabilityRegion
      // Must be a positive number
      var found = false
      do {

        minStability = if (minStability <= 0) currentResolution else minStability
//        // begin with a big resolution and make it smaller when smaller bounds are found
        println(s"Simulating min:$minStability, max:$maxStability, reso:$currentResolution")
//         In the first the simulations aren't split instead different parameters are parallelized.
        val valuesToTest = BigDecimal(minStability) to BigDecimal(maxStability) by
          BigDecimal(currentResolution)
        val rdd = sc.parallelize(
          valuesToTest, valuesToTest.length)
          .map(stabilityChange => {
            // parametersToChange should be unique on each worker so it should possible to change
            // it here
            parametersToChange = parametersToChange.zipWithIndex.map{
              case (str, idx) => if (idx == 0) str else stabilityChange.toString}
            var arrival = arrivalSpec
            var service = serviceSpec
            checkStability match {
              case "S" => service = parametersToChange
              case "A" => arrival = parametersToChange
            }
//            arrivalSpec.foreach(a => System.err.println(s"arrival + $a"))
            SparkSimulator.doSimulation(options.get, 1, new FJDataSummerizer(1,
            1000, parametersToChange),arrival,service)
          })
        val dataSummerizer = rdd.collect()
        var allStable = true
        for (summerizer <- dataSummerizer) summerizer match {
          case summerizer: FJDataSummerizer => {
            println(s"summerizer reso:$currentResolution ${summerizer.params.mkString(" ")}, " +
              s"unstable:${summerizer.isUnstable}")
            if (summerizer.isUnstable) {
              maxStability = BigDecimal(Math.min(summerizer.params(1).toDouble, maxStability)
              ).setScale((1/currentResolution).toInt, BigDecimal.RoundingMode.UP).toDouble
              allStable = false
            }
            else
              minStability = BigDecimal(Math.max(summerizer.params(1).toDouble, minStability)
              ).setScale((1/currentResolution).toInt, BigDecimal.RoundingMode.DOWN).toDouble
          }
        }

        if (currentResolution == stabilityResolution || allStable) {
          val outPath = new Path(outfile_base)
          val fs = FileSystem.get(URI.create(outfile_base), new Configuration())
          if (fs.exists(outPath)) {
            fs.delete(outPath, true)
          }
          spark.createDataFrame(rdd.zipWithIndex().flatMap{
            case (f, sliceIdx) => (0 until f.num_samples).map(i => {
                      f match {
                        case f: FJDataSummerizer => {
                          Row(
                            sliceIdx,
                            i,
                            f.job_waiting_d(i),
                            f.job_sojourn_d(i),
                            f.job_service_d(i),
                            f.job_cputime_d(i),
                            f.maxSojournTimeIncreasing,
                            parametersToChange(1)
                          )}
                        //          case _ => Row()
                      }
                    })
                    }, metricType).write.csv(outfile_base) // metricType
          found = true
          println(s"The stability region is between [$minStability, $maxStability], " +
            s"allStable: $allStable")
        } else
          currentResolution = Math.max(currentResolution / 2, stabilityResolution) // breaks the loop
        // for
        // testing
      } while (!found)
      //.toDF().cache
//      rdd.write.parquet(outfile_base)
    }
//    val rdd = sc.parallelize(0 until num_slices, num_slices).map((s: Integer) => doSimulation
//    (foptions, s)).cache
    //		List<FJDataAggregator> dl = rdd.collect();
    //System.out.println("rdd = "+rdd);
    //System.out.println("dl = "+dl);
    // write out the path data, if it was recorded
//    println(rdd.count())
//    val df = rdd.toDF()
  try
      Thread.sleep(1000)
    catch {
      case e: InterruptedException =>
        val formatter = new HelpFormatter
        formatter.printHelp("FJSimulator", getCliOptions)
        e.printStackTrace()
    }
    spark.stop()
  }
}