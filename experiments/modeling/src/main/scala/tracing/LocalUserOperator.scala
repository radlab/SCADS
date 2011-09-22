package edu.berkeley.cs
package scads
package piql
package modeling

import piql.exec._
import piql.plans._
import scadr._

import collection.mutable.ArrayBuffer

case class LocalUserOperator() extends QueryPlan

class LocalUserExecutor extends ParallelExecutor {
  override def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = plan match {
    case LocalUserOperator() => {
      new QueryIterator {
        val name = "LocalUserOperator"
	      private var iterator: Iterator[Tuple] = null

	      private def toUser(idx: Int) = ArrayBuffer(new User("User%010d".format(idx)))
	      private def randomUser(numUsers: Int) = toUser(scala.util.Random.nextInt(numUsers) + 1)

        def open: Unit = {
	        val numTuples = ctx.parameters(0).asInstanceOf[Int]
	        val maxUserId = ctx.parameters(1).asInstanceOf[Int]
	        iterator = Array.fill(numTuples)(randomUser(maxUserId)).iterator
        }
        def close: Unit =
          iterator = null

        def hasNext = iterator.hasNext
        def next = iterator.next
      }
    }
    case otherPlan => super.apply(otherPlan)
  }
}
