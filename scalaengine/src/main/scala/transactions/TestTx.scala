package edu.berkeley.cs.scads.storage.examples
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord

import scala.actors.Actor._

import edu.berkeley.cs.scads.storage.transactions.FieldAnnotations._

import java.lang.annotation.Documented
import annotation.target.field

case class KeyRec(var x: Int) extends AvroRecord

case class ValueRec(var s: String,
                    @FieldGT(1)
                    @FieldGE(2)
                    @FieldLT(3)
                    @FieldLE(4)
                    var i: Int,
                    @FieldGT(1)
                    @FieldGE(1)
                    @FieldLT(4)
                    @FieldLE(4)
                    var a: Long,
                    var b: Float,
                    var c: Double) extends AvroRecord

class TestTx {
  def run() {
    val cluster = TestScalaEngine.newScadsCluster()

    val ns = new SpecificNamespace[KeyRec, ValueRec]("testns", cluster, cluster.namespaces) with Transactions[KeyRec, ValueRec] {
      override val protocolType = TxProtocol2pc()
    }
    ns.open()

    ns.put(KeyRec(1), ValueRec("A", 1, 1, 1.0.floatValue, 1.0))
    ns.put(KeyRec(2), ValueRec("B", 1, 1, 1.0.floatValue, 1.0))
    ns.put(KeyRec(3), ValueRec("C", 1, 1, 1.0.floatValue, 1.0))
    ns.put(KeyRec(4), ValueRec("D", 1, 1, 1.0.floatValue, 1.0))

    val tx1 = new Tx(100) ({
      List.range(5, 5 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("G", 1, 1, 1.0.floatValue, 1.0)))
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx1.Execute()

    val tx2 = new Tx(100) ({
      List.range(9, 9 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("H", 1, 1, 1.0.floatValue, 1.0)))
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx2.Execute()

    val tx3 = new Tx(100) ({
      List.range(7, 7 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("I", 1, 1, 1.0.floatValue, 1.0)))
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx3.Execute()

    // Delete something
    println("delete 1")

//    ns.put(KeyRec(1), None)

    val tx4 = new Tx(100) ({
      // need to read your writes...
      ns.get(KeyRec(1))
      ns.put(KeyRec(1), None)
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx4.Execute()

    println("    get 1")
    println(ns.get(KeyRec(1)))
    println("    get range")
    ns.getRange(None, None).foreach(x => println(x))

    // Logical updates
    println("running some logical updates")
    val tx5 = new Tx(100) ({
      ns.putLogical(KeyRec(12), ValueRec("", 2, 3, 2.1.floatValue, 0.2))
    }).Accept(0.90) {
    }.Commit( success => {
    })
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
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
