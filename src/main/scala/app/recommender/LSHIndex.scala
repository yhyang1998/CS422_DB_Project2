package app.recommender

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {
  private val minhash = new MinHash(seed)

  var title_kw_map = data.map(x => (x._3, x))

  def hash_per_row(input: List[String]): IndexedSeq[Int] = {
    minhash.hash(input)
  }
  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets()
    : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    // each title_kw correspond to a IndexedSeq[Int], which represents its signature
    val title_sig_map: RDD[(List[String], IndexedSeq[Int])] = hash(data.map(x => x._3)).map(x => (x._2, x._1)).distinct()

//    title_sig_map.foreach(x => println(x))
//    println("==========")

    var res = title_sig_map.join(title_kw_map).map(x => {
      (x._2._1, x._2._2)
    }).groupBy(_._1).mapValues(_.map(_._2).toList)
//    res.foreach(x => println(x))
    res
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    var query_map = queries.map(x => (x._1, x._2))
    var buckets = getBuckets()
    var res = queries.leftOuterJoin(buckets).map(x => {
      var res_title = List[(Int, String, List[String])]()
      if(x._2._2 != None){  // found something
        res_title = x._2._2.get
      }
      (x._1, x._2._1, res_title)
    })
    res
  }
}
