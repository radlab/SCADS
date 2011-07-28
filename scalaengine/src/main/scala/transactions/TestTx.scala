package edu.berkeley.cs.scads.storage.examples

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord

import scala.actors.Actor._

case class KeyRec(var x: Int) extends AvroRecord
case class ValueRec(var s: String) extends AvroRecord

class TestTx {
  def run() {
    val cluster = TestScalaEngine.newScadsCluster()

    val ns = new SpecificNamespace[KeyRec, ValueRec]("testns", cluster, cluster.namespaces) with Transactions[KeyRec, ValueRec]
    ns.open()

    ns.put(KeyRec(1), ValueRec("A"))
    ns.put(KeyRec(2), ValueRec("B"))
    ns.put(KeyRec(3), ValueRec("C"))
    ns.put(KeyRec(4), ValueRec("D"))

    val tx1 = new Tx(100) ({
      List.range(5, 5 + 4).foreach(x => ns.put(KeyRec(x), ValueRec("G")))
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx1.ExecuteMain()
    tx1.PrepareTest()
    ns.getRange(None, None).foreach(x => println(x))

    val tx2 = new Tx(100) ({
      List.range(9, 9 + 4).foreach(x => ns.put(KeyRec(x), ValueRec("H")))
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx2.ExecuteMain()
    tx2.PrepareTest()
    ns.getRange(None, None).foreach(x => println(x))

    val tx3 = new Tx(100) ({
      List.range(7, 7 + 4).foreach(x => ns.put(KeyRec(x), ValueRec("I")))
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx3.ExecuteMain()
    tx3.PrepareTest()
    ns.getRange(None, None).foreach(x => println(x))

    // Run the commits
    tx2.CommitTest()
    println("getrange")
    ns.getRange(None, None).foreach(x => println(x))
    tx3.CommitTest()
    println("getrange")
    ns.getRange(None, None).foreach(x => println(x))
    tx1.CommitTest()
    println("getrange")
    ns.getRange(None, None).foreach(x => println(x))

    // Delete something
    println("delete 1")

    ns.put(KeyRec(1), None)

    // // read set is not gathered yet, so inserts work for now.
    // val tx4 = new Tx(100) ({
    //   ns.put(KeyRec(1), None)
    // }).Accept(0.90) {
    // }.Commit( success => {
    // })
    // tx4.ExecuteMain()
    // tx4.PrepareTest()
    // tx4.CommitTest()

    println("    get 1")
    println(ns.get(KeyRec(1)))
    println("    get range")
    ns.getRange(None, None).foreach(x => println(x))
  }
}

object TestTx {
  def main(args: Array[String]) {
    val test = new TestTx()
    test.run()
    println("Exiting...")
    System.exit(0)
  }
}
