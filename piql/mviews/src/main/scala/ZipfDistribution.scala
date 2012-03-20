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

trait Samplable {
  val cdf: Array[Double]

  def icdf(p: Double): Int = {
    var idx = Arrays.binarySearch(cdf, p)
    if (idx < 0) {
      idx = -(idx+1)
    }
    idx
  }

  def sample(implicit rnd: Random): Int = {
    icdf(rnd.nextDouble)
  }
}

class ZipfDistribution(val n: Int, val alpha: Double) extends Samplable {

  // sum frequencies
  val zeta = (1 until n+1).map(1d / pow(_, alpha))
    .foldLeft[List[Double]](0 :: Nil)((sums, x) => x + sums.head :: sums)

  // normalize
  val cdf = zeta.reverse.map(_ / zeta.head).tail.toArray

  /**
   * Fills in discrete intervals in CDF with another distribution
   * to a specified granularity (default .1%) to save memory.
   * Useful for creating balanced partitioning schemes.
   */
  def interpolate(interior: Array[Double], granularity: Double = 0.001) = {
    var interpolated = List[Double]()
    var source = List[Tuple2[Int,Int]]()

    (-1 until cdf.length - 1).foreach(i => {
      val base = if (i == -1) 0 else cdf(i)
      val gap = cdf(i+1) - base
      interpolated ::= base
      source ::= (i+1,0)
      if (gap > granularity) {
        (0 until interior.length - 1).foreach(j => {
          val offset = interior(j)
          if (interior(j+1) - offset > granularity) {
            val p = base + offset * gap
            interpolated ::= p
            source ::= (i+1,j+1)
          }
        })
      }
    })

    interpolated ::= 1.0
    source ::= (cdf.length - 1, 0)

    new Samplable {
      val cdf = interpolated.reverse.tail.toArray
      val rcdf = source.reverse.tail.toArray

      def fineSample(p: Double): Tuple2[Int,Int] = {
        rcdf(icdf(p))
      }
    }
  }
}
