package edu.berkeley.cs.scads
package piql
package mviews

import scala.util.Random
import scala.collection.mutable.HashMap
import java.util.Arrays
import scala.math._

object ZipfDistribution {
  private val insts = new HashMap[Tuple2[Int,Double],ZipfDistribution]()
  
  def getDistribution(n: Int, alpha: Double) = {
    insts.getOrElseUpdate((n,alpha), new ZipfDistribution(n, alpha))
  }

  def sample(n: Int, alpha: Double)(implicit rnd: Random): Int = {
    insts.getOrElseUpdate((n,alpha), new ZipfDistribution(n, alpha)).sample
  }
}

class ZipfDistribution(val n: Int, val alpha: Double) {

  // sum frequencies
  val zeta = (1 until n+1).map(1d / pow(_, alpha))
    .foldLeft[List[Double]](0 :: Nil)((sums, x) => x + sums.head :: sums)

  // normalize
  val cdf = zeta.reverse.map(_ / zeta.head).tail.toArray

  def sample(p: Double): Int = {
    var idx = Arrays.binarySearch(cdf, p)
    if (idx < 0) {
      idx = -(idx+1)
    }
    idx
  }

  def sample(implicit rnd: Random): Int = {
    sample(rnd.nextDouble)
  }
}
