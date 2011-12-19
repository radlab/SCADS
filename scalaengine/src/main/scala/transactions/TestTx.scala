package edu.berkeley.cs.scads.storage.examples
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.{AvroRecord, AvroPair}

import scala.actors.Actor._

import edu.berkeley.cs.scads.storage.transactions.FieldAnnotations._

case class KeyRec(var x: Int) extends AvroRecord

case class ValueRec(var s: String,
                    @FieldGT(0)
                    @FieldGE(0)
                    @FieldLT(100)
                    @FieldLE(100)
                    var i: Int,
                    @FieldGT(0)
                    @FieldGE(0)
                    @FieldLT(100)
                    @FieldLE(100)
                    var a: Long,
                    var b: Float,
                    var c: Double) extends AvroRecord

case class DataRecord(var id: Int) extends AvroPair {
  var s: String = _
  @FieldGE(0)
  @FieldLT(20)
  var a: Int = _
  @FieldGE(0)
  @FieldLT(20)
  var b: Long = _
  var c: Float = _

  override def toString = "DataRecord(" + id + ", " + s + ", " + a + ", " + b + ", " + c + ")"
}

class TestTx {
  def run() {
    val cluster = TestScalaEngine.newScadsCluster(4)

//    val ns = new SpecificNamespace[KeyRec, ValueRec]("testns", cluster, cluster.namespaces) with Transactions[KeyRec, ValueRec] {
//     override lazy val protocolType = NSTxProtocol2pc()
//    }
//    ns.open()
//    ns.setPartitionScheme(List((None, cluster.getAvailableServers)))

    val nsPair = cluster.getNamespace[DataRecord]("testnsPair", NSTxProtocolMDCC())
    nsPair.setPartitionScheme(List((None, cluster.getAvailableServers)))
    Thread.sleep(1000)
    var dr = DataRecord(1)
    dr.s = "a"; dr.a = 1; dr.b = 1; dr.c = 1.0.floatValue
    nsPair.put(dr)
    dr.id = 2
    nsPair.put(dr)

    val nbRecords = 1

    new Tx(300) ({
      List.range(3, 3 + nbRecords ).foreach(x => {
        dr.s = "b"
        dr.id = x
        nsPair.put(dr)
      })
    }).Execute()

    Thread.sleep(1000)

    new Tx(1000) ({
      List.range(3, 3 + nbRecords ).foreach(x => {
        dr.s = "c"
        dr.id = x
        nsPair.put(dr)
      })
    }).Execute()

    // Sleep for a little bit to wait for the commits.
    Thread.sleep(1000)

    nsPair.getRange(None, None).foreach(x => println(x))
    println("nsPair.getRecord 3: " + nsPair.getRecord(DataRecord(3)))
    println("nsPair.getRecord 4: " + nsPair.getRecord(DataRecord(4)))
    Thread.sleep(100000)
/*
   new Tx(100) ({
     ns.put(KeyRec(1), ValueRec("A", 1, 1, 1.0.floatValue, 1.0))
     ns.put(KeyRec(2), ValueRec("B", 1, 1, 1.0.floatValue, 1.0))
     ns.put(KeyRec(3), ValueRec("C", 1, 1, 1.0.floatValue, 1.0))
     ns.put(KeyRec(4), ValueRec("D", 1, 1, 1.0.floatValue, 1.0))
   }).Execute()

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
     List.range(7, 7 + 4).foreach(x => {
       ns.get(KeyRec(x))
       ns.put(KeyRec(x), ValueRec("I", 1, 1, 1.0.floatValue, 1.0))})
   }).Accept(0.90) {
   }.Commit( success => {
   })
   tx3.Execute()

   // Delete something
   println("delete 1")

   ns.put(KeyRec(1), None)

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
*/
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
