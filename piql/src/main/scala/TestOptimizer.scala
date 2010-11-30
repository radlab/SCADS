package edu.berkeley.cs
package scads
package piql
package test

import avro.marker._
import storage.TestScalaEngine

case class R1(var f1: Int) extends AvroPair
case class R2(var f1: Int, var f2: Int) extends AvroPair

object Relations {
  val cluster = TestScalaEngine.newScadsCluster()
  //TODO: Fix variance of namespace to remove cast
  //TODO: subout namespace so we don't need to invoke zookeeper etc.
  val r1 = cluster.getNamespace[R1]("r1").asInstanceOf[Namespace]
  val r2 = cluster.getNamespace[R2]("r2").asInstanceOf[Namespace]
  val r2Prime = cluster.getNamespace[R2]("r2Prime").asInstanceOf[Namespace]
}

object TestOptimizer {
  import Relations._

  def main(args: Array[String]): Unit = {
    Optimizer(r1.where("f1".a === (0.?)))
  }
}
