package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val rddFromFile = sc.textFile("src/main/resources" + path).map( f=>{
      f.split('|')
    })

    rddFromFile.map(line => line match {
      case Array(uid, tid, rate, ts) => (uid.toInt, tid.toInt, None, rate.toDouble, ts.toInt)
      case Array(uid, tid, pre_rate, rate, ts) => (uid.toInt, tid.toInt, Some(pre_rate.toDouble), rate.toDouble, ts.toInt)

    })
  }
}
