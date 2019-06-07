package forkulator.Helper

import org.apache.spark.SparkContext

class SparkHelper {

}

object SparkHelper {
  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
  }

  def currentNumberOfActiveExecutors(sc: SparkContext): Int = {
    currentActiveExecutors(sc).size
  }
}