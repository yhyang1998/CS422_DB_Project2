package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex : LSHIndex) extends Serializable {
  var cache: Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]] = null
  var histogram: Map[IndexedSeq[Int], Int] = null   // it records the number of time that this signature has been queried within a period of time
  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {
    if(histogram != null){
      val hist_sum = histogram.values.sum
      val to_cache = histogram.filter(k => ( k._2.toDouble / hist_sum) * 100 > 1)
      val rdd = sc.parallelize(histogram.toSeq).filter(f => to_cache.contains(f._1))
      cache = sc.broadcast(lshIndex.lookup(rdd).map(f => (f._1, f._3)).collect().toMap)
    }
  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]) = {
    // ext is a map that map the signature and the title
    cache = ext
  }

  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {
    // take the queries -> check the cache and if it's a hit, put the result in the first rdd, if it's not return the
    // the RDD of (signature, keyword list) pairs to lookup fo the normal lookup
    val rdd1 =
      if(cache != null)
        queries.map(x => (lshIndex.hash_per_row(x) , x)).filter(z => cache.value.contains(z._1)).map(k =>(k._2, cache.value(k._1)) )
      else
        null
    val rdd2 =
      if (cache != null)
        queries.map(x => (lshIndex.hash_per_row(x),x)).filter(z => !cache.value.contains(z._1))
      else
        queries.map(x => (lshIndex.hash_per_row(x),x))

    (rdd1, rdd2)


//    val rdd1 = if(cache != null) {
//      lshIndex.hash(queries).filter(z => cache.value.contains(z._1)).map(k =>(k._2, cache.value(k._1)) )
//    } else
//      null
//    val rdd2 = if (cache != null)
//      lshIndex.hash(queries).filter(z => !cache.value.contains(z._1))
//    else {
//      lshIndex.hash(queries)
////      queries.map(k => (lshIndex.hash(k),k))
//    }
//    (rdd1, rdd2)
  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val lst = queries.collect()
    histogram = lst.foldLeft(Map[IndexedSeq[Int], Int]()){
      (m,k) => {
        val hash = lshIndex.hash_per_row(k)
        if (m.contains(hash))
          m + (hash -> (m(hash) + 1))
        else
          m + (hash -> 1)
      }
    }

    if(cache != null) {
      val rdd1 = queries.map(x => (lshIndex.hash_per_row(x), x)).filter(z => cache.value.contains(z._1)).map(k => (k._2, cache.value(k._1)))
      val tmp = queries.map(x => (lshIndex.hash_per_row(x), x)).filter(z => !cache.value.contains(z._1))
      val rdd2 = lshIndex.lookup(tmp).map(z => (z._2, z._3))

      rdd1.union(rdd2)
    }
    else {
      val tmp = queries.map(x => (lshIndex.hash_per_row(x), x))

      lshIndex.lookup(tmp).map(z => (z._2, z._3))
    }

//    var (cachehit_rdd, cachemiss_rdd) = cacheLookup(queries)
//    val res_lookup = lshIndex.lookup(cachemiss_rdd).map(x => (x._2, x._3))
//
//    res_lookup
  }
}
