package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */

  var title_map: RDD[(Int,(String, List[String]))] = null //(tid, (name, keywords))
  var rating_map: RDD[(Int, (Option[Double], Double))] = null  //(tid, (old rating, new rating))
  var avg_rating_map:  RDD[(Int, (Double, Int))] = null  // (tid, (sum, num))
  var kw_rating: RDD[(Int, Double)] = null //avg
  var title_rating: RDD[(Int, (String, Double))] = null

  /* For update usage */

  var updated_map: RDD[(Int, (Option[Double], Double))] = null

  def sum_seqOp = (u: Double, v: (Option[Double], Double)) => {
    if (v._1 != None) u - v._1.get + v._2
    else u + v._2
  }
  def sum_combOp = (u1: Double,  u2: Double) => u1 + u2

  def num_seqOp = (u: Int, v: (Option[Double], Double)) => {
    if(v._1 != None) u // has been calculated before
    else u+1
//    u+1
  }
  def num_combOp = (u1: Int,  u2: Int) => u1 + u2

  def init(
            ratings : RDD[(Int, Int, Option[Double], Double, Int)],
            title : RDD[(Int, String, List[String])]
          ) : Unit = {

    /* for title rating */
    rating_map = ratings.map(r => (r._2, (r._3, r._4))) //(tid, (old_rating, new_rating))
    title_map = title.map(t => (t._1, (t._2, t._3))) //(tid , (name, kw))

    val rating_sum = rating_map.aggregateByKey(0.0)(sum_seqOp, sum_combOp)
    val rating_num = rating_map.aggregateByKey(0)(num_seqOp, num_combOp)

    avg_rating_map = rating_sum.join(rating_num).map(x => (x._1, (x._2._1, x._2._2)))  // (tid, (sum, num))


    title_rating = title_map.leftOuterJoin(avg_rating_map).map(x => {
      var rate = 0.0
      if (x._2._2 != None) rate = x._2._2.get._1 / x._2._2.get._2
      (x._1, (x._2._1._1, rate))
    })


  }
  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult() : RDD[(String, Double)] = {
//    println("====title rate====")
//    title_rating.map(x => (x._2._1, x._2._2)).foreach(x => println(x))
    title_rating.map(x => (x._2._1, x._2._2))
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords : List[String]) : Double = {

    val filtered_title_map = title_map.filter(x => keywords.forall(x._2._2.contains))
    var avg_kw_rate = -1.0

    if(filtered_title_map.count() != 0){
      kw_rating = filtered_title_map.join(avg_rating_map).map(x => {
        var rate = 0.0
        if(x._2._2 != None) rate = x._2._2._1 / x._2._2._2
        (x._1, rate)  //in case that the title is not rated
      })

      avg_kw_rate = kw_rating.map(_._2).sum() / kw_rating.count()
      avg_kw_rate
    }else{
      avg_kw_rate
    }

  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = {

    val deltaRDD = sc.parallelize(delta_)
    updated_map = deltaRDD.map(r => (r._2, (r._3, r._4)))
//    println("====update map====")
//    updated_map.foreach(x => println(x))

//    updated map also needs aggregation
    val upd_rating_sum = updated_map.aggregateByKey(0.0)(sum_seqOp, sum_combOp)
    val upd_rating_num = updated_map.aggregateByKey(0)(num_seqOp, num_combOp)
    val upd_rating_avg = upd_rating_sum.join(upd_rating_num).map(x => (x._1, (x._2._1, x._2._2)))  // (tid, (sum, num))

//    println("====update avg====")
//    upd_rating_avg.foreach(x => println(x))
//    println("====origin avg====")
//    avg_rating_map.foreach(x => println(x))

    avg_rating_map = avg_rating_map.fullOuterJoin(upd_rating_avg).map(x => {
      var sum = 0.0
      var num = 0
      if(x._2._2 != None){ // it can either be a new rating that never exists in the avg_rating_map or a updated value
        if(x._2._1 == None){
          sum = x._2._2.get._1  //updated sum
          num = x._2._2.get._2  //updated num
        }else{
          sum = x._2._2.get._1 + x._2._1.get._1 //updated sum + origin sum
          num = x._2._2.get._2 + x._2._1.get._2 //updated num
        }
      } else {
        sum = x._2._1.get._1  //origin sum
        num = x._2._1.get._2  //origin num
      }

      (x._1, (sum, num))
    })

//    println("====updated avg====")
//    avg_rating_map.foreach(x => println(x))


    title_rating = title_map.leftOuterJoin(avg_rating_map).map(x => {
      var rate = 0.0

      if (x._2._2 != None) rate = x._2._2.get._1 / x._2._2.get._2
      (x._1, (x._2._1._1, rate))
    })

//    println("====title rating====")
//    title_rating.foreach(x => println(x))



  }
}
