package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.nodes.TestableBdbStorageNode
import edu.berkeley.cs.scads.nodes.TestableSimpleStorageNode

import edu.berkeley.cs.scads.thrift.KeyStore
import edu.berkeley.cs.scads.thrift.RangeSet
import edu.berkeley.cs.scads.thrift.Record
import edu.berkeley.cs.scads.thrift.RecordSetType
import edu.berkeley.cs.scads.thrift.RecordSet

import scala.util.Random

object KeyStoreUtils {

  def putNumericKeys(s: Int, count: Int, ns: String, connection: KeyStore.Client) {
    val e = s+count
    for (i <- s to e) {
      val nk = new NumericKey(i)
      val rec = new Record(nk.serialize,nk.serialize)
      connection.put(ns,rec)
    }
  }
  
  /*
  def assertNumericKeys(s: Int, count: Int, ns: String, connection: KeyStore.Client) {
    val e = s+count
    for (i <- 1 to e) {
      val nk = new NumericKey(i)
      assert(connection.get(ns,nk.serialize).value === nk.serialize)
    }
  }
  */
}

abstract class KeyStoreSuite extends Suite { 
  val connection: KeyStore.Client

  // == Simple get/put tests ==
  def testSimpleGetPut() = {
    def rec = new Record("test", "test")
    connection.put("simple", rec)
    assert(connection.get("simple", "test") === rec)
  }

  def testUpdate() = {
    /*
    for (i <- (1 to 100)) {
      val rec = new Record("test"+i,"data"+i)
      connection.put("tu",rec)
    }
    * */
    KeyStoreUtils.putNumericKeys(0,100,"tu",connection)
    for (i <- (0 to 100)) {
      val nk = new NumericKey(i)
      assert(connection.get("tu",nk.serialize).value === nk.serialize)
    }

    for (i <- (0 to 100)) {
      val nk = new NumericKey(i)
      val rec = new Record(nk.serialize,"data"+(i*2))
      connection.put("tu",rec)
    }
    for (i <- (0 to 100)) {
      val nk = new NumericKey(i)
      assert(connection.get("tu",nk.serialize).value === ("data"+(i*2)))
    }
  }

  def testSetToNull() = {
    for (i <- (1 to 100)) {
      val rec = new Record("test"+i,"data"+i)
      connection.put("tstn",rec)
    }
    for (i <- (1 to 100)) {
      if ((i % 2) == 0) {
        val rec = new Record("test"+i,null)
        connection.put("tstn",rec)
      }
    }
    for (i <- (1 to 100)) {
      if ((i % 2) == 0)
        assert(connection.get("tstn","test"+i).value === null)
      else
        assert(connection.get("tstn","test"+i).value === ("data"+i))
    }
  }

  def testEmptyString() = {
    for (i <- (1 to 100)) {
      val rec = new Record("test"+i,"data"+i)
      connection.put("tes",rec)
    }
    for (i <- (1 to 100)) {
      if ((i % 2) == 0) {
        val rec = new Record("test"+i,"")
        connection.put("tes",rec)
      }
    }
    for (i <- (1 to 100)) {
      if ((i % 2) == 0)
        assert(connection.get("tes","test"+i).value === "")
      else
        assert(connection.get("tes","test"+i).value === ("data"+i))
    }
  }


  // == test various get_set functionality ==
  def testSimpleGetSet() = {
    for (i <- (1 to 100)) {
      val nk = new NumericKey(i)
      val rec = new Record(nk.serialize,nk.serialize)
      connection.put("tsgs",rec)
    }
    val rangeSet = new RangeSet(new NumericKey(0).serialize,new NumericKey(20).serialize,0,0)
    rangeSet.unsetOffset()
    rangeSet.unsetLimit()
    val targetSet = new RecordSet(RecordSetType.RST_RANGE,
                                  rangeSet,null,null)
                                  
    val res: java.util.List[Record] = connection.get_set("tsgs",targetSet)
    assert(res.size() === 20)
    // would be nice if there was a foreach with position
    for (i <- 1 to 20) {
      val nk = new NumericKey(i)
      assert(res.get(i-1).value === nk.serialize)
    }
  }

  def testGetSetLimit() {
    KeyStoreUtils.putNumericKeys(0,100,"tgsl",connection)
    
    val lim = (new Random).nextInt(90)+1
    val rangeSet = new RangeSet(null,null,0,lim)
    val targetSet = new RecordSet(RecordSetType.RST_RANGE,
                                  rangeSet,null,null)
    val res: java.util.List[Record] = connection.get_set("tgsl",targetSet)
    assert(res.size() === lim)
    for (i <- 0 to (lim-1)) {
      val nk = new NumericKey(i)
      assert(res.get(i).value === nk.serialize)
    }
  }

  def testGetSetOffset() {
    KeyStoreUtils.putNumericKeys(0,100,"tgso",connection)
    
    val off = (new Random).nextInt(50)+1
    val rangeSet = new RangeSet(null,null,off,0)
    rangeSet.unsetLimit()
    val targetSet = new RecordSet(RecordSetType.RST_RANGE,
                                  rangeSet,null,null)
    val res: java.util.List[Record] = connection.get_set("tgso",targetSet)
    assert(res.size() === (101-off))
    for (i <- off to 100) {
      val nk = new NumericKey(i)
      assert(res.get(i-off).value === nk.serialize)
    }
  }

}

class BdbKeyStoreTest extends KeyStoreSuite {
  val n = new TestableBdbStorageNode()
  val connection = n.createConnection()
}

class SimpleKeyStoreTest extends KeyStoreSuite {
  val n = new TestableSimpleStorageNode()
  val connection = n.createConnection()
}
