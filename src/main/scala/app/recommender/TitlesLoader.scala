package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class TitlesLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val rddFromFile = sc.textFile("src/main/resources" + path).map( f=>{
      f.split('|')
    })

    rddFromFile.map(line => line match {
      case Array(id, str) if (line.length == 2) => Tuple3(id.toInt, str, List.empty[String])
      case Array(id, str, _*) => Tuple3(id.toInt, str, line.drop(2).toList)

    })



  }
}
