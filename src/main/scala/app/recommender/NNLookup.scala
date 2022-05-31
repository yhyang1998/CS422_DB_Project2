package app.recommender

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val title_sig = lshIndex.hash(queries)
    val res_lookup = lshIndex.lookup(title_sig).map(x => (x._2, x._3))

    res_lookup
  }
}
