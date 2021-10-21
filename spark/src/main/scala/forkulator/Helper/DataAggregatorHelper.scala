package forkulator.Helper

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class DataAggregatorHelper {

}

object DataAggregatorHelper {
  val stabilityMetricType = StructType(
    Seq(
      StructField(name = "sliceNum", dataType = LongType, nullable = true),
      StructField(name = "maxSojournTimeIncreasing", dataType = IntegerType, nullable = true),
      StructField(name = "isUnstable", dataType = BooleanType),
      StructField(name = "param", dataType = StringType, nullable = true)
    )
  )
  val metricType = StructType(
    Seq(
      StructField(name = "sliceNum", dataType = LongType, nullable = true),
      StructField(name = "sampleNum", dataType = IntegerType, nullable = true),
      StructField(name = "waitingTime", dataType = DoubleType, nullable = true),
      StructField(name = "sojournTime", dataType = DoubleType, nullable = true),
      StructField(name = "serviceTime", dataType = DoubleType, nullable = true),
      StructField(name = "cpuTime", dataType = DoubleType, nullable = true),
//      StructField(name = "idleTime", dataType = DoubleType, nullable = true),
      StructField(name = "inOrderSojournTime", dataType = DoubleType, nullable = true),
      StructField(name = "maxSojournTimeIncreasing", dataType = IntegerType, nullable = true),
      StructField(name = "param", dataType = StringType, nullable = true)
    )
  )

  val metricTypeWithVariance = StructType(
    Seq(
      StructField(name = "sliceNum", dataType = LongType, nullable = false),
      StructField(name = "sampleNum", dataType = IntegerType, nullable = false),
      StructField(name = "waitingTime", dataType = DoubleType, nullable = false),
      StructField(name = "sojournTime", dataType = DoubleType, nullable = false),
      StructField(name = "serviceTime", dataType = DoubleType, nullable = false),
      StructField(name = "cpuTime", dataType = DoubleType, nullable = false),
      StructField(name = "maxSojournTimeIncreasing", dataType = IntegerType, nullable = false),
      StructField(name = "param", dataType = StringType, nullable = false),
      StructField(name = "waitingTimeVar", dataType = DoubleType, nullable = false),
      StructField(name = "sojournTimeVar", dataType = DoubleType, nullable = false),
      StructField(name = "serviceTimeVar", dataType = DoubleType, nullable = false)
    )
  )

  def avgMetricType(df: DataFrame): Option[DataFrame] = {
    val spark = SparkSession.getActiveSession
    if (spark.nonEmpty) {
      val spark_ = spark.get
      import spark_.implicits._
      Some(df.groupBy('sliceNum, 'sampleNum).agg(avg('waitingTime).as('waitingTime), avg
      ('sojournTime).as('sojournTime),
        stddev_samp('sojournTime).as('sojournTimeStddev),
        avg('serviceTime).as('serviceTime), count(lit(1)).alias("Num Of Records"),
        org.apache.spark.sql.functions.min('param).as('param)).select($"*").sort("sampleNum"))
    }
    else
      None
  }

  def countIncr(df: DataFrame, filterStr: String): (Double, Int) = {
    val spark = SparkSession.getActiveSession
    if (spark.nonEmpty) {
      val spark_ = spark.get
      import spark_.implicits._
      // Need to find max increasing
      var cnt = 0
      var maxCnt = 0
      var acc = 0.0
      var param = -1.0
      df.filter(filterStr).collect.foreach { a =>
        // println(a.getAs[Double]("sojournTime"))
        val curr = a.getAs[Double]("sojournTime")
        if (curr > acc)
          cnt += 1
        else {
          maxCnt = maxCnt.max(cnt)
          cnt = 0
        }
        param = a.getAs[String]("param").toDouble
        acc = curr
      }
      (param, maxCnt.max(cnt))
    } else
      (-1,-1)
  }

  def getStabilityRegion(df: DataFrame): ((Double, Double), String) = {
    val spark = SparkSession.getActiveSession
    if (spark.nonEmpty) {
      val spark_ = spark.get
      import spark_.implicits._

      val minT = DataAggregatorHelper.countIncr(df, "sliceNum == 0")
      val maxT = DataAggregatorHelper.countIncr(df, "sliceNum == 1")
      var unclear = ""
      var (resultMin, resultMax) = (minT._1, maxT._1)
      if (minT._2 >= 10) {
        println(s"min increased $minT")
        unclear += "|min"
      }
      if (maxT._2 < 10) {
        println(s"max increased $maxT")
        unclear += "|max"
      }
      // println( (resultMin, resultMax))
      ((resultMin, resultMax), unclear)
    } else {
      ((-1, -1), "sparkSession not accessible")
    }
  }
}