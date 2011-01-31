package edu.berkeley.cs
package scads
package piql
package test

import avro.marker._
import storage.TestScalaEngine

case class R1(var f1: Int) extends AvroPair {
  var v = 1 //HACK
}
case class R2(var f1: Int, var f2: Int) extends AvroPair

object Relations {
  val cluster = TestScalaEngine.newScadsCluster()
  //TODO: Fix variance of namespace to remove cast
  //TODO: subout namespace so we don't need to invoke zookeeper etc.
  val r1 = cluster.getNamespace[R1]("r1")
  val r2 = cluster.getNamespace[R2]("r2")
  val r2Prime = cluster.getNamespace[R2]("r2Prime")
}

object TestOptimizer {
  def main(args: Array[String]): Unit = {
    implicit val executor = new SimpleExecutor with DebugExecutor

    val cluster = TestScalaEngine.newScadsCluster()
    val r1 = cluster.getNamespace[R1]("r1")
    val r2 = cluster.getNamespace[R2]("r2")
    val r2Prime = cluster.getNamespace[R2]("r2Prime")

    r1 ++= (1 to 10).map(i => R1(i))
    val query = r1.where("f1".a === 1).toPiql()
    println(query())

    for(i <- (1 to 10); j <- (1 to 10)) {
      val r = R2(i,j)
      r2.put(r.key, r.value)
    }

    println(r2.getRange(None,None))
     val query2 = r2.where("f1".a === 1).limit(0.?, 10).toPiql()
    println(query2(1))
    println(query2(2))


    System.exit(0)
  }
}
