// Inspired from:
// https://www.scitepress.org/papers/2017/65150/65150.pdf
// http://erikerlandson.github.io/blog/2015/05/06/parallel-k-medoids-using-scala-parseq/
// https://gist.github.com/erikerlandson/c3c35f0b1aae737fc884
// https://github.com/letiantian/kmedoids/blob/master/kmedoids.py
// https://github.com/tdebatty/spark-kmedoids
def clusteringScoreMedoids[T <: org.apache.spark.sql.Row, U >: T](data: RDD[T], k: Int) = {

  // Distance measurement
  def metric(r1: T, r2: T) = {
    def checkType(cls: Any) = cls match {
      case i: Int => i.toDouble
      case d: Double => d
      case _ => 1.0
    }

    val sum = (0 until r1.length).map(k => math.pow(checkType(r1(k)) - checkType(r2(k)), 2)).sum
    math.sqrt(sum)
  }

  val sample = data.collect()
  var medoids = Random.shuffle(sample.toSet).take(k).toSeq


  require(medoids.length == k)

  val measureItemOther = (x: T, mv: Seq[T]) => mv.view.zipWithIndex.map { z => (metric(x, z._1), z._2) }.min

  var itr = 1
  val nbIter = 40
  var halt = itr > nbIter
  var lastMetric = sample.map { x => measureItemOther(x, medoids)._1 }.sum

  // Not functional at all
  // I'm sorry
  while (!halt) {

    val dataDistanceAll = Array.fill[ArrayBuffer[T]](k)(new ArrayBuffer[T]())
    sample.foldLeft(dataDistanceAll)((dm, x) => {
      dm(measureItemOther(x, medoids)._2) += x
      dm
    })

    medoids = dataDistanceAll.filter(_.nonEmpty).map { clust =>
      clust.minBy { e =>
        clust.foldLeft(0.0)((x, v) => x + metric(v, e))
      }
    }

    val newMetric = sample.map { x => measureItemOther(x, medoids)._1 }.sum

    // Stop if no improvement
    if (lastMetric == newMetric) halt = true
    lastMetric = newMetric

    itr += 1
    if (itr > nbIter) halt = true
  }

  (medoids.toList, lastMetric)
}



// Testing
// Build subset
val n = numericOnly.count()
val sampleSize = n.toInt
val sample = numericOnly.sample(withReplacement=false, 10000.0 / n.toDouble).cache()
val res = (20 to 100 by 20).map(k => {
  (k, clusteringScoreMedoids(sample.rdd, k))
}).foreach(println)
