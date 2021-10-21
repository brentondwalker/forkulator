package forkulator

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.log4j.{Level, LogManager, Logger, PropertyConfigurator}
import java.util
import java.util.ArrayList

import scala.collection.JavaConverters._
import forkulator.randomprocess.{IntertimeProcess, IntervalPartition, RerunIntertimeProcess}
import org.apache.commons.cli.{CommandLine, CommandLineParser, HelpFormatter, OptionBuilder, Options, ParseException, PosixParser}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.util.concurrent.ThreadLocalRandom

import forkulator.Helper.{DataAggregatorHelper, SparkHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel
import org.json4s.scalap.Error

import scala.collection.mutable.{ArrayBuffer, ListBuffer}



//object Holder extends Serializable {
//  @transient lazy val log = Logger.getLogger(getClass.getName)
//}
class RerunSparkMetrics {


}

object RerunSparkMetrics {
  val REPLICATIONS = 50

  def load(spark: SparkSession, inputDir: String, fileFormat: Option[String] = None): DataFrame = {

    val p = new Path(inputDir)
    if(!p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)) {
      throw new Exception(s"Error: $inputDir does not exist!")
    }

    val inputFormat = fileFormat match {
      case None => inputDir.split('.').last
      case Some(s) => s
    }

    inputFormat match {
      case "parquet" => spark.read.parquet(inputDir)
      case "orc" => spark.read.orc(inputDir)
      //      case Formats.avro => spark.read.avro(inputDir)
      case "json" => spark.read.json(inputDir)
      case "csv" | _ => spark.read.option("inferSchema", "true").option("header", "true").csv(inputDir) //if unspecified, assume csv
    }
  }

  def getCliOptions: Options = {
    val cli_options = new Options
    cli_options.addOption("h", "help", false, "print help message")
    cli_options.addOption("w", "numworkers", true, "number of workers/servers")
    cli_options.addOption("t", "numtasks", true, "number of tasks per job")
    cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run")
    cli_options.addOption("i", "samplinginterval", true, "samplig interval")
    cli_options.addOption("p", "savepath", true, "save some iterations of the simulation path (arrival time, service time etc...)")
    cli_options.addOption("s", "numslices", true, "the number of slices to divide te job into.  This is ideally a multiple of the number of cores.")
    cli_options.addOption("f", "metricFile", true, "the metric file " +
      "from a spark experiment to rerun task times in the simulator.")
    cli_options.addOption("rm", "rerunMode", true, "the mode decides which data is used to rerun " +
      "the spark experiment. g: generated data, sr: spark task runtimes.")
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
    OptionBuilder.hasArgs
    OptionBuilder.withLongOpt("overheadprocess")
    OptionBuilder.withDescription("overhead process")
    cli_options.addOption(OptionBuilder.create("O"))
    OptionBuilder.hasArgs
    OptionBuilder.withLongOpt("secondoverheadprocess")
    OptionBuilder.withDescription("second overhead process")
    cli_options.addOption(OptionBuilder.create("Os"))
    OptionBuilder.hasArgs
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
    * Parses one line of a metrics file. The format should be columns with delimiter \t
    * and have the following data
    * jobNum generatedTimes sparkTaskRuntimes sparkDeserializeTimes jobWaitingTime
    * @param input
    * @return
    */
  def parseMetricFile(input: String): (ArrayBuffer[Double], ArrayBuffer[Double],
    ArrayBuffer[Double], ArrayBuffer[Double], ArrayBuffer[Double], Double) = {
    val parts = input.split("\t")
    // Header line. Ignore, we are using plain strings of csv file.
    if(parts(1) == "generatedTimes")
      (ArrayBuffer[Double](), ArrayBuffer[Double](), ArrayBuffer[Double](), ArrayBuffer[Double](), ArrayBuffer[Double](), 0.0)
    else
      (parts(1).split(",").map(_.toDouble/1000).to[ArrayBuffer], // generatedTimes
        parts(2).split(",").map(_.toDouble/1000).to[ArrayBuffer], // sparkTaskRuntimes
        parts(3).split(",").map(_.toDouble/1000).to[ArrayBuffer], // sparkDeserializeTimes
        parts(4).split(",").map(_.toDouble/1000).to[ArrayBuffer], // sparkSerializeTimes
        parts(6).split(",").map(_.toDouble/1000).to[ArrayBuffer], // duration
        parts(5).toDouble/1000) // jobWaitingTime

  }

  /**
    * Runs the simulation. It might be a better idea to get the simulation as parameter instead of
    * the CommandLine options and values to alter them
    * @param options
    * @param segment_index
    * @param aggregator
    * @param arrival_process
    * @param service_process
    * @param numOfSampleDivider Divides the number of sample requested by this value.
    * @return
    */
  def doSimulation(options: CommandLine, segment_index: Int, aggregator: FJBaseDataAggregator,
                   arrival_process: RerunIntertimeProcess, service_process: RerunIntertimeProcess,
                   numOfSampleDivider: Int = 1, overhead_process: RerunIntertimeProcess = null, second_overhead_process: RerunIntertimeProcess = null, server_overhead_process: RerunIntertimeProcess = null)
  : FJBaseDataAggregator = {
    val num_workers = options.getOptionValue("w").toInt
    val num_tasks = options.getOptionValue("t").toInt
    val sampling_interval = 1
    val outfile_base = options.getOptionValue("o")
//    val inputMetricsFile = options.getOptionValue("f")
    // compute how many samples, and how many jobs are needed from each slice (round up)

    val num_samples = arrival_process.getSampleNum
    val samples_per_slice = Math.ceil(num_samples.toDouble).toInt
    val jobs_per_slice = samples_per_slice.toLong
    // data aggregator
    val data_aggregator: FJBaseDataAggregator = aggregator.getNewInstance(samples_per_slice,
      aggregator.batch_size)
    // optional path logger
    // when running on Spark we only do the path logging for the first slice
    if ((segment_index == 0) && options.hasOption("p")) data_aggregator.path_logger = new FJPathLogger(options.getOptionValue("p").toInt, num_tasks)
    // simulator
    val server_queue_spec = options.getOptionValues("q")
//    val oldSparkMetrics = RerunSparkMetrics.load(spark = oldSparkMetrics, inputMetricsFile)

    val sim = new FJSimulator(server_queue_spec, num_workers, num_tasks, arrival_process,
      service_process, null, 0, data_aggregator, overhead_process, second_overhead_process, server_overhead_process)
    sim.job_type = null
    // start the simulator running...
    sim.run(jobs_per_slice, sampling_interval, false)
    sim.event_queue.clear() // Should not be necessary. Only for tests
    sim.data_aggregator
  }

  def shuffleArray(ar: ArrayBuffer[Double]) = {
    val rnd = ThreadLocalRandom.current
    var i = ar.length - 1
    while(i > 0) {
      val index = rnd.nextInt(i + 1)
      // Simple swap
      val a = ar(index)
      ar(index) = ar(i)
      ar(i) = a
      i -= 1
    }
    ar
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
      options = Some(parser.parse(RerunSparkMetrics.getCliOptions, args))
    catch {
      case e: ParseException =>
        val formatter = new HelpFormatter
        formatter.printHelp("FJSimulator", RerunSparkMetrics.getCliOptions)
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
    var outfile_base = options.map(_.getOptionValue("o")).getOrElse("out")
    val outPath = new Path(outfile_base)
    val fs = FileSystem.get(URI.create(outfile_base), new Configuration())
    if (fs.exists(outPath)) {
      outfile_base += System.currentTimeMillis()
//      fs.delete(outPath, true)
    }
    // distribute the simulation segments to workers
//    val ar = new util.ArrayList[Integer](num_slices)
    if (checkStability.equals("")) {
      val inputMetricsFile = options.get.getOptionValue("f")
      val rerunMode = options.get.getOptionValue("rm")
      println(s"Reading $inputMetricsFile")
      val rddInput = sc.textFile(inputMetricsFile, minPartitions=num_slices)
//      val df = load(spark, inputDir = inputMetricsFile)
      // Needs much memory on long runs but spark cannot combine multiple rows easily so
      // it's the only possibility
//      val taskTimesLocal = df.collect

      val out = rddInput.mapPartitions(a => {
        val t1 = System.currentTimeMillis()
        val sb: StringBuilder = new StringBuilder()
        val wList:ListBuffer[String] = ListBuffer()
        val generatedTaskTimes:ListBuffer[Double] = ListBuffer()
        val sparkTaskTimes:ListBuffer[Double] = ListBuffer()
        val sparkDuration:ListBuffer[Double] = ListBuffer()
        val sparkDeserializeTime:ListBuffer[Double] = ListBuffer()
        val jobInterarrivalTimes:ListBuffer[Double] = ListBuffer()
        var c = 0
        var start = ""
        var end = ""
        while( a.hasNext ) {
          // sb.setLength(0)
          val str = a.next
          val parsed = parseMetricFile(str)
          if(parsed._6 > 0.0) {
            generatedTaskTimes ++= parsed._1 //shuffleArray
            sparkTaskTimes ++= parsed._2
            sparkDuration ++= parsed._5
//            sparkTaskTimes ++= (parsed._2.zip(parsed._3).map((x:(Double, Double)) => {
//              x._1 + x._2}))
            sparkDeserializeTime ++= parsed._3
            jobInterarrivalTimes += parsed._6
          }
          // val parts = str.split("\t")
          // // wList += ( sb.append(">>").append(str.slice(0,20)).append("<<").toString() )
          // end = parts(1)
          // if( c == 0 ) {
          //     start = s"S$end"
          // }
          // c += 1
          // wList2 += s"${parts(1)}"
          // wList += str.slice(0,5)
        }
        //   wList += sb.append(start).append("|").append(end).append("").toString
        //   wList += s"$start|$end|$c"
//        wList += wList2.toString
//        val tmp: List[java.lang.Double] = generatedTaskTimes.toList.map(d => {java.lang.Double
//          .valueOf(d)})
        val interarrivalProcess = new RerunIntertimeProcess(jobInterarrivalTimes.toList.map
        (Double.box _).asJava)
        val serviceProcess = rerunMode match {
          case "g" =>
            println("Using generated times")
            new RerunIntertimeProcess(generatedTaskTimes.toList.map (Double.box _).asJava)
          case "s" =>
            println("Using sparkS times")
            new RerunIntertimeProcess(sparkTaskTimes.toList.map (Double.box _).asJava)
          case "d" =>
            println("Using spark duration times")
            new RerunIntertimeProcess(sparkDuration.toList.map (Double.box _).asJava)
          case _ => throw new Exception(s"Service process type unknown.")
        }
        val num_samples = interarrivalProcess.getSampleNum
        val samples_per_slice = Math.ceil(num_samples.toDouble).toInt
        val jobs_per_slice = samples_per_slice.toLong
        val t2 = System.currentTimeMillis()
        println(s"This slice has $num_samples samples. Time to read file ${t2-t1}")
        val out = List[FJBaseDataAggregator](RerunSparkMetrics.doSimulation(options.get, 0, new
            FJDataAggregator(num_samples), arrival_process = interarrivalProcess, serviceProcess))
          .iterator
        println(s"Time needed for simulation ${System.currentTimeMillis() - t2}")
        out
//        wList.iterator
      }).zipWithIndex().flatMap{case (f, sliceIdx) => (0 until f.max_samples).map(i => {
              f match {
                case f: FJDataAggregator => Row(
                  sliceIdx,
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
      }

      val rdd = spark.createDataFrame(out, DataAggregatorHelper.metricType)
//      val rdd = spark.createDataFrame(sc.parallelize(0 until num_slices, num_slices)
//      val rdd = spark.createDataFrame(sc.parallelize(0 until num_slices, num_slices).map(s => {
////        var generatedTimes: Option[Array[Double]] = None
////        for(i <- 0 until 1) {
////          generatedTimes = Some(tmp(i)(1).asInstanceOf[String].split(",").map(_.toDouble))
////        }
//        RerunSparkMetrics.doSimulation(options.get, s, new FJDataAggregator(num_slices))
//      })
//        .zipWithIndex().flatMap{case (f, sliceIdx) => (0 until f.max_samples).map(i => {
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
//      }, DataAggregatorHelper.metricType)//.toDF().cache
      rdd.write.parquet(outfile_base)
    }
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