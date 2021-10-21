package forkulator

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

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
//import java.util.logging.LogManager

import forkulator.Helper.{DataAggregatorHelper, SparkHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel



object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
class SparkSimulator {


}

object SparkSimulator {
  val REPLICATIONS = 10
  val HIGHEST_STABILITY = 1.0
  val BATCH_SIZE = 1000

  def getCliOptions: Options = {
    val cli_options = new Options
    cli_options.addOption("h", "help", false, "print help message")
    cli_options.addOption("w", "numworkers", true, "number of workers/servers")
    cli_options.addOption("t", "numtasks", true, "number of tasks per job")
    cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run")
    cli_options.addOption("i", "samplinginterval", true, "samplig interval")
    cli_options.addOption("p", "savepath", true, "save some iterations of the simulation path (arrival time, service time etc...)")
    cli_options.addOption("s", "numslices", true, "the number of slices to divide the job into.  This is ideally a multiple of the number of cores.")
    cli_options.addOption("lb", "lowerbound", true, "the lower bound to check if the lower utilization is stable. If more than this percentage of simulations were unstable the utilization is increased.")
    cli_options.addOption("ub", "upperbound", true, "the upper bound to to check if the upper utilization is unstable. If less than this percentage of simulations were stable the utilization is decreased.")
    cli_options.addOption("r", "replications", true, "the number of replications per parameter to check for stability.")
    OptionBuilder.hasArgs
    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("queuetype")
    OptionBuilder.withDescription("queue type and arguments")
    cli_options.addOption(OptionBuilder.create("q"))
    //    cli_options.addOption(OptionBuilder.withLongOpt("queuetype").hasArgs().isRequired
    //      .withDescription("queue type and arguments").create("q"))
    OptionBuilder.hasArgs
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
    OptionBuilder.hasArgs
//    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("overheadprocess")
    OptionBuilder.withDescription("overhead process")
    cli_options.addOption(OptionBuilder.create("O"))
    OptionBuilder.hasArgs
    //    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("secondoverheadprocess")
    OptionBuilder.withDescription("second overhead process")
    cli_options.addOption(OptionBuilder.create("Os"))
    OptionBuilder.withLongOpt("serveroverheadprocess")
    OptionBuilder.withDescription("server overhead process")
    cli_options.addOption(OptionBuilder.create("OS"))

    //    cli_options.addOption(OptionBuilder.withLongOpt("serviceprocess").hasArgs.isRequired.withDescription("service process").create("S"))
    OptionBuilder.hasArgs
    //    OptionBuilder.isRequired
    OptionBuilder.withLongOpt("jobpartition")
    OptionBuilder.withDescription("job_partition")
    cli_options.addOption(OptionBuilder.create("J"))
    OptionBuilder.hasArgs
    OptionBuilder.withLongOpt("jobpartitiontype")
    OptionBuilder.withDescription("job_partition_type")
    cli_options.addOption(OptionBuilder.create("Jp"))
    OptionBuilder.hasArgs
    OptionBuilder.withLongOpt("check_stability")
    OptionBuilder.withDescription("check_stability")
    cli_options.addOption(OptionBuilder.create("cs"))
    cli_options
  }

  /**
    * Runs the simulation. It might be a better idea to get the simulation as parameter instead of
    * the CommandLine options and values to alter them
    * @param options
    * @param segment_index
    * @param aggregator
    * @param arrivalSpec
    * @param serviceValues
    * @param numOfSampleDivider Divides the number of sample requested by this value.
    * @return
    */
  def doSimulation(options: CommandLine, segment_index: Int, aggregator: FJBaseDataAggregator,
                   arrivalSpec: Array[String] = Array.empty, serviceValues: Array[String] = Array
    .empty, numOfSampleDivider: Int = 1, overheadValues: Array[String] = Array.empty)
  : FJBaseDataAggregator = {
    val server_queue_type = options.getOptionValue("q")
    val num_workers = options.getOptionValue("w").toInt
    val num_tasks = options.getOptionValue("t").toInt
    val num_samples = options.getOptionValue("n").toLong / numOfSampleDivider
    var num_slices = 1
    if (options.hasOption("s")) num_slices = options.getOptionValue("s").toInt
    val sampling_interval = options.getOptionValue("i").toInt
    val outfile_base = options.getOptionValue("o")
    // compute how many samples, and how many jobs are needed from each slice (round up)
    val samples_per_slice = Math.ceil(num_samples.toDouble / num_slices).toInt
    val jobs_per_slice = samples_per_slice.toLong// * sampling_interval.toLong

    val arrival_process = FJSimulator.parseProcessSpec(if (arrivalSpec.isEmpty) options
      .getOptionValues("A") else arrivalSpec)
    // figure out the service process
//    val service_process_spec = options.getOptionValues("S")
    val service_process = FJSimulator.parseProcessSpec(if (serviceValues.isEmpty) options
      .getOptionValues("S") else serviceValues)
    var overhead_process:forkulator.randomprocess.IntertimeProcess = null
    overhead_process = FJSimulator.parseProcessSpec(if (overheadValues.isEmpty) options.getOptionValues("O") else overheadValues)
    var second_overhead_process:forkulator.randomprocess.IntertimeProcess = null
    second_overhead_process = FJSimulator.parseProcessSpec(if (overheadValues.isEmpty) options.getOptionValues("Os") else overheadValues)
    var server_overhead_process:forkulator.randomprocess.IntertimeProcess = null
    server_overhead_process = FJSimulator.parseProcessSpec(if (overheadValues.isEmpty) options.getOptionValues("OS") else overheadValues)
    // if we are in job-partitioning mode, figure out the partitioning type
    var job_partition_process: Option[IntervalPartition] = None
    if (options.hasOption("J")) {
      val job_partition_spec = options.getOptionValues("J")
      job_partition_process = Some(FJSimulator.parseJobDivisionSpec(job_partition_spec))
    }
    var job_partition_type: Option[FJPartitionJob] = None
    if (options.hasOption("Jp")) {
      val job_partition_spec = options.getOptionValues("Jp")
      job_partition_type = Some(FJSimulator.parseJobPartitionTypeSpec(job_partition_spec))
    } else {
      job_partition_type = Some(new FJRandomPartitionJob)
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
      service_process, job_partition_process.orNull, 0, data_aggregator, overhead_process, second_overhead_process, server_overhead_process)
    sim.job_type = job_partition_type.orNull
    // start the simulator running...
    sim.run(jobs_per_slice, sampling_interval, true)
    sim.event_queue.clear() // Should not be necessary. Only for tests
    sim.data_aggregator
  }

  def main(args: Array[String]): Unit = {
//    val log = LogManager.getRootLogger
//    args.map(a => log.warn(a))
//    args.map(a => println(a))
    println(args.mkString(" "))
    val conf = new SparkConf().setAppName("forkulator") //.setMaster(master);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array[Class[_]](classOf[FJPathLogger], classOf[FJDataAggregator]))
    val spark = new sql.SparkSession.Builder().appName("forkulator").config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._


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
    var stabilityDecimalNumber = 1
    if (options.exists(_.hasOption("s"))) num_slices = options.get.getOptionValue("s")
      .toInt
    if (options.exists(_.hasOption("cs"))) {
      val stabilityOptions = options.get.getOptionValues("cs")
      checkStability = stabilityOptions(0)
      stabilityRegion = stabilityOptions(1).toDouble
      stabilityResolution = stabilityOptions(2).toDouble
      val integerPlaces = stabilityOptions(2).indexOf('.')
      stabilityDecimalNumber = stabilityOptions(2).length - integerPlaces - 1
    }
    val num_samples = options.get.getOptionValue("n").toLong
    var outfile_base = options.map(_.getOptionValue("o")).getOrElse("out")
    val outPath = new Path(outfile_base)
    val fs = FileSystem.get(URI.create(outfile_base), new Configuration())
    if (fs.exists(outPath)) {
      outfile_base += System.currentTimeMillis()
//      fs.delete(outPath, true)
    }



    // distribute the simulation segments to workers
    val ar = new util.ArrayList[Integer](num_slices)
    if (checkStability.equals("")) {
      spark.createDataFrame(sc.parallelize(0 until num_slices, num_slices)
        .map(s => SparkSimulator.doSimulation(options.get, s, new FJDataAggregator((num_samples/num_slices).toInt)))
        .mapPartitionsWithIndex(
          (sliceIdx, baseDataIterator) => {
            baseDataIterator.flatMap(f => (0 until f.num_samples).map(i => {
              f match {
                case f: FJDataAggregator => Row(
                  sliceIdx.toLong,
                  i,
                  f.job_start_time(i) - f.job_arrival_time(i),
                  f.job_departure_time(i) - f.job_arrival_time(i),
                  f.job_completion_time(i) - f.job_start_time(i),
                  f.job_cpu_time(i),
                  f.job_inorder_departure_time(i) - f.job_arrival_time(i),
                  0.toInt,
                  ""
                )
                //          case _ => Row()
              }
            })
            )
          }
        ), DataAggregatorHelper.metricType).write.parquet(outfile_base)



//      val rdd =
//        spark.createDataFrame(sc.parallelize(0 until num_slices, num_slices)
//        .map(s => SparkSimulator.doSimulation(options.get, s, new FJDataAggregator((num_samples/num_slices).toInt)))
//        .zipWithIndex().flatMap{case (f, sliceIdx) => (0 until f.num_samples).map(i => {
//        f match {
//          case f: FJDataAggregator => Row(
//            sliceIdx,
//            i,
//            f.job_start_time(i) - f.job_arrival_time(i),
//            f.job_departure_time(i) - f.job_arrival_time(i),
//            f.job_completion_time(i) - f.job_start_time(i),
//            f.job_cpu_time(i),
//            f.job_inorder_departure_time(i) - f.job_arrival_time(i),
//            0.toInt,
//            ""
//          )
////          case _ => Row()
//        }
//      })
//      }, DataAggregatorHelper.metricType).write.parquet(outfile_base)//.toDF().cache
//      rdd.write.parquet(outfile_base)
    } else {
      println(checkStability)

      var replications = REPLICATIONS
      if (options.get.hasOption("r")) replications = options.get.getOptionValue("r").toInt
      var lowerBound = 0.6
      if (options.get.hasOption("lb")) lowerBound = options.get.getOptionValue("lb").toDouble
      var upperBound = 0.8
      if (options.get.hasOption("ub")) upperBound = options.get.getOptionValue("ub").toDouble
      val arrivalSpec = options.get.getOptionValues("A")
      val serviceSpec = options.get.getOptionValues("S")
      var parametersToChange = Array.empty[String]
      checkStability match {
        case "S" => parametersToChange = serviceSpec
        case "A" => parametersToChange = arrivalSpec
      }
      var currentResolution = if (stabilityResolution > 0.5) stabilityResolution else 0.5
      var minStability = (parametersToChange(1).toDouble - stabilityRegion).max(0.1)
      var maxStability = parametersToChange(1).toDouble + stabilityRegion
      var allStable = true
      // Must be a positive number
      var found = false
      do {
        minStability = if (minStability <= 0) currentResolution else minStability

        // Dividing number of executors by 2 to get 2 tasks per executor
        if (!allStable)
          currentResolution = currentResolution.min(
            ((BigDecimal(maxStability)-BigDecimal(minStability))/
              (SparkHelper.currentNumberOfActiveExecutors(sc)*2).max(1)).toDouble)
        currentResolution = currentResolution.max(stabilityResolution)
//        // begin with a big resolution and make it smaller when smaller bounds are found
        println(s"Simulating min:$minStability, max:$maxStability, reso:$currentResolution/$stabilityResolution")
//         In the first the simulations aren't split instead different parameters are parallelized.
        var valuesToTest = ((BigDecimal(minStability) to BigDecimal(maxStability) by
          BigDecimal(currentResolution)).toSet + BigDecimal(maxStability)).toSeq
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
            // Run a smaller sample set to get the stabilization bounds which speeds up the
            // simulation.
            val summerizer = SparkSimulator.doSimulation(options.get, 1, new FJDataSummerizer(Math.ceil(num_samples / BATCH_SIZE).toInt,
              BATCH_SIZE, parametersToChange),arrival,service, 1)
            summerizer
          })
        val dataSummerizer = rdd.collect()
        allStable = true
        for (summerizer <- dataSummerizer) summerizer match {
          case summerizer: FJDataSummerizer => {
            println(s"summerizer param ${summerizer.params.mkString(" ")}, reso:$currentResolution, " +
              s"unstable:${summerizer.isUnstable}, ${(1/currentResolution).toInt}, " +
              s"maxInc: ${summerizer.maxSojournTimeIncreasing}/${summerizer.num_samples}")
            println(s"summerizer size ${summerizer.job_service_mean.length}")
            if (summerizer.isUnstable) {
              maxStability = BigDecimal(Math.min(summerizer.params(1).toDouble, maxStability)
              ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.UP).toDouble
              allStable = false
            }
            else
              minStability = BigDecimal(Math.max(summerizer.params(1).toDouble, minStability)
              ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.DOWN).toDouble
          }
        }
        if(minStability > maxStability) {
          // Can happen because the stability depends on the random variable
          val tmp = maxStability
          maxStability = minStability
          minStability = tmp
        } else {
          if (currentResolution == stabilityResolution && !allStable) {
            found = true
            println(s"The stability region is between [$minStability, $maxStability], " +
              s"allStable: $allStable")
          } else {
            if (!allStable)
              currentResolution = Math.max(currentResolution / 2, stabilityResolution) // breaks the loop
            else {
              minStability = maxStability
              maxStability += currentResolution // To work with imprecisely floats
            }
          }
        }

        // for
        // testing
      } while (!found)
//      if (found) {
//        println(s"Stability region is between ${minStability} and ${maxStability}.")
//        //.write.csv(outfile_base) // metricType
//        //          df.write.parquet(outfile_base)
//        // TODO Don't use static value
//        val outCsv = new BufferedWriter(new FileWriter("/mnt/out/stability.csv", true))
//        outCsv.write(s"${outfile_base.split("/").last}_no_double_check,${minStability},${maxStability}\n")
//        outCsv.close()
//      }
      var numOfTries = 0
      // Double check the stability. This does seem to create a lot of resource problems so it is not used curerntly
      do {
        found = true
        if ((maxStability - minStability) < currentResolution)
          maxStability = BigDecimal(minStability + currentResolution
          ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.UP).toDouble
        // To be sure that the results are right, repeat the found bounds multiple times.
        println(s"Simulating min:$minStability, max:$maxStability, reso:$currentResolution," +
          s" $replications repetitions per stability. Lower bound: $lowerBound, upper bound: $upperBound")

        var rddMinStability = sc.parallelize(0 until replications).map(replication => {
          parametersToChange = parametersToChange.zipWithIndex.map {
            case (str, idx) => if (idx == 0) str else minStability.toString
          }
          var arrival = arrivalSpec
          var service = serviceSpec
          checkStability match {
            case "S" => service = parametersToChange
            case "A" => arrival = parametersToChange
          }
          (replication, SparkSimulator.doSimulation(options.get, 1, new
              FJDataSummerizer(num_samples.toInt,
                BATCH_SIZE, parametersToChange), arrival, service))
        })
        val dfMinStability =
          spark.createDataFrame(rddMinStability.map{
            f => {
              f._2 match {
                case summerizer: FJDataSummerizer => {
                  Row(
                    f._1.toLong,
                    summerizer.maxSojournTimeIncreasing,
                    summerizer.isUnstable,
                    summerizer.params(1)
                  )}
//                case _ => Row()
              }
            }
          }, DataAggregatorHelper.stabilityMetricType)

        var rddMaxStability = sc.parallelize(0 until replications).map(replication => {
          parametersToChange = parametersToChange.zipWithIndex.map {
            case (str, idx) => if (idx == 0) str else maxStability.toString
          }
          var arrival = arrivalSpec
          var service = serviceSpec
          checkStability match {
            case "S" => service = parametersToChange
            case "A" => arrival = parametersToChange
          }
          (replication, SparkSimulator.doSimulation(options.get, 1, new
              FJDataSummerizer(num_samples.toInt,
                BATCH_SIZE, parametersToChange), arrival, service))
        })
        val dfMaxStability =
          spark.createDataFrame(rddMaxStability.map{
            f => {
              f._2 match {
                case summerizer: FJDataSummerizer => {
                  Row(
                    f._1.toLong,
                    summerizer.maxSojournTimeIncreasing,
                    summerizer.isUnstable,
                    summerizer.params(1)
                  )}
                 case _ => Row()
              }
            }
          }, DataAggregatorHelper.stabilityMetricType)

        val collectedMinStability = dfMinStability.collect
        val numOfMinUnstableSimulations = collectedMinStability.count(p => p.getAs[Boolean]("isUnstable"))
        val collectedMaxStability = dfMaxStability.collect
        val numOfMaxUnstableSimulations = collectedMaxStability.count(p => p.getAs[Boolean]("isUnstable"))
        println(s"$numOfMinUnstableSimulations / ${collectedMinStability.length} simulations with params $minStability were unstable.\n"
               +s"$numOfMaxUnstableSimulations / ${collectedMaxStability.length} simulations with params $maxStability were unstable")
        if (numOfTries >= 3 && found && Math.abs(numOfMaxUnstableSimulations - numOfMinUnstableSimulations) < 0.2 * replications && currentResolution < 5 * stabilityResolution) {
          println(s"Number of higher utilization resulted in less equal than lower utilization. Increasing stabiltiy resolution.")
          found = false
          // TODO Remove current resolution because it is not necessary anymore
          maxStability = BigDecimal(maxStability + stabilityResolution
          ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.UP).toDouble
          currentResolution += stabilityResolution
          numOfTries = 0
        } else {
          numOfTries += 1
          if (numOfMaxUnstableSimulations < collectedMaxStability.length * upperBound) {
            found = false
            println(s"It seems that the value $maxStability is stable. Rising it.")
//            minStability = maxStability
            //          maxStability += stabilityResolution
            minStability = BigDecimal(minStability + stabilityResolution
            ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.HALF_UP).toDouble
            maxStability = BigDecimal(maxStability + stabilityResolution
            ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.HALF_UP).toDouble
          }else
          if (numOfMinUnstableSimulations >= collectedMinStability.length * lowerBound) {
            found = false
            println(s"It seems that the value $minStability is unstable. Lowering it.")
//            maxStability = minStability
            maxStability = BigDecimal(maxStability - stabilityResolution
            ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.HALF_UP).toDouble
            //          minStability -= stabilityResolution
            minStability = BigDecimal(minStability - stabilityResolution
            ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.HALF_UP).toDouble
          }
        }

        // Check here if found and set the variable
        if (found) {
          println(s"Stability region is between $minStability and $maxStability.")
          //.write.csv(outfile_base) // metricType
          //          df.write.parquet(outfile_base)
          // TODO Don't use static value
          val outCsv = new BufferedWriter(new FileWriter("/mnt/out/stability.csv", true))
          outCsv.write(s"${outfile_base.split("/").last},$minStability,$maxStability\n")
          outCsv.close()
        }



//        val rdd = sc.parallelize(0 until REPLICATIONS * 2)
//          .map(replication => {
//            // parametersToChange should be unique on each worker so it should possible to change
//            // it here
//            parametersToChange = parametersToChange.zipWithIndex.map {
//              case (str, idx) => if (idx == 0) str else if ((replication / REPLICATIONS) == 0)
//                minStability.toString else maxStability.toString
//            }
//            var arrival = arrivalSpec
//            var service = serviceSpec
//            checkStability match {
//              case "S" => service = parametersToChange
//              case "A" => arrival = parametersToChange
//            }
//            (replication / REPLICATIONS, SparkSimulator.doSimulation(options.get, 1, new
//                FJDataSummerizer(Math.ceil(num_samples / BATCH_SIZE).toInt,
//                  BATCH_SIZE, parametersToChange), arrival, service))
//          })//.persist(StorageLevel.MEMORY_AND_DISK_SER)
//        println(s"Running simulations ${rdd.count()}")
//        val df =
//          spark.createDataFrame(rdd.flatMap{
//            f => (0 until f._2.num_samples).map(i => {
//              f._2 match {
//                case summerizer: FJDataSummerizer => {
//                  Row(
//                    f._1.toLong,
//                    i,
//                    summerizer.job_waiting_mean(i),
//                    summerizer.job_sojourn_mean(i),
//                    summerizer.job_service_mean(i),
//                    summerizer.job_cputime_mean(i),
////                    summerizer.worker_idle_time_mean(i),
//                    summerizer.job_inorder_sojourn_mean(i),
//                    summerizer.maxSojournTimeIncreasing,
//                    summerizer.params(1)
//                  )}
//                //          case _ => Row()
//              }
//            })
//          }, DataAggregatorHelper.metricType) //.persist(StorageLevel.MEMORY_ONLY)
//        println(s"Got ${df.count()} partitions")
        // TODO: Either don't use Option or check for it
//        val dfMeans = DataAggregatorHelper.avgMetricType(df)
//        dfMeans.get.persist(StorageLevel.MEMORY_AND_DISK)
//        println(s"Got ${dfMeans.get.count()} means")
//        val minT = DataAggregatorHelper.countIncr(dfMeans.get, "sliceNum == 0")
//        val maxT = DataAggregatorHelper.countIncr(dfMeans.get, "sliceNum == 1")
//        dfMeans.get.unpersist()
//        println(s"Counted Increments")
//        if (minT._2 >= (REPLICATIONS-1)) {
//          found = false
//          println(s"It seems that the value $minStability is unstable. Lowering it.")
//          maxStability = minStability
////          minStability -= stabilityResolution
//          minStability = BigDecimal(minStability - stabilityResolution
//          ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.DOWN).toDouble
//        } else if (maxT._2 < (REPLICATIONS-1)) {
//          found = false
//          println(s"It seems that the value $maxStability is stable. Rising it.")
//          minStability = maxStability
////          maxStability += stabilityResolution
//          maxStability = BigDecimal(maxStability + stabilityResolution
//          ).setScale(stabilityDecimalNumber, BigDecimal.RoundingMode.DOWN).toDouble
//        }
//        // Check here if found and set the variable
//        if (found) {
//          println(s"Stability region is between ${minT._1} and ${maxT._1}.")
//          //.write.csv(outfile_base) // metricType
////          df.write.parquet(outfile_base)
//          // TODO Don't use static value
//          val outCsv = new BufferedWriter(new FileWriter("/mnt/out/stability.csv", true))
//          outCsv.write(s"${outfile_base.split("/").last},${minT._1},${maxT._1}\n")
//          outCsv.close()
//        }
      } while (!found)


//      print(rdd.flatMap{
//        f => (0 until f._2.num_samples).map(i => {
//          f._2 match {
//            case summerizer: FJDataSummerizer => {
//              Row(
//                0, //f._1,
//                0, //i,
//                summerizer.job_waiting_mean(i),
//                summerizer.job_sojourn_mean(i),
//                summerizer.job_service_mean(i),
//                summerizer.job_cputime_mean(i),
//                0, //summerizer.maxSojournTimeIncreasing,
//                summerizer.params(1)
//              )}
//            //          case _ => Row()
//          }
//        })
//      }.collect()(0)) //.collect.map(a => a.schema))
//      println(df.collect()(0))
//      df.groupBy('sliceNum).agg(avg(struct('waitingTime, 'sojournTime, 'serviceTime,
//        'cpuTime))).select($"*")
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