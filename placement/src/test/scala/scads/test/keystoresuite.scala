package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.model.IntegerField
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.nodes.TestableBdbStorageNode
import edu.berkeley.cs.scads.nodes.TestableSimpleStorageNode

import edu.berkeley.cs.scads.thrift.ExistingValue
import edu.berkeley.cs.scads.thrift.KeyStore
import edu.berkeley.cs.scads.thrift.Language
import edu.berkeley.cs.scads.thrift.RangeSet
import edu.berkeley.cs.scads.thrift.Record
import edu.berkeley.cs.scads.thrift.RecordSetType
import edu.berkeley.cs.scads.thrift.RecordSet
import edu.berkeley.cs.scads.thrift.TestAndSetFailure
import edu.berkeley.cs.scads.thrift.UserFunction

import scala.util.Random

import scala.actors.Future
import scala.actors.Futures.future

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
  val node: StorageNode
  val connection: KeyStore.Client

  // == Simple get/put tests ==
  def testSimpleGetPut() = {
    def rec = new Record("test", "test")
    connection.put("simple", rec)
    assert(connection.get("simple", "test") === rec)
  }

  def testUpdate() = {
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


  // == test_and_set testing (and setting) ==
  def testAndSetNull() = {
    val ev = new ExistingValue("n",0)
    ev.unsetValue()
    ev.unsetPrefix()
    val rec = new Record("tasnull","tasnull")
    val res = connection.test_and_set("tasn",rec,ev)
    assert(res)
  }

  def testAndSetNullFail() = {
    val ev = new ExistingValue("n",0)
    ev.unsetValue()
    ev.unsetPrefix()
    val rec = new Record("tasnullf","tasnullf")
    connection.put("tasnf",rec)
    val res = 
      try {
        connection.test_and_set("tasnf",rec,ev)
      } catch {
        case tsf: TestAndSetFailure => 
          {
            assert(tsf.currentValue === "tasnullf")
            true
          }
        case e: Exception => false
      }
    assert(res)
  }

  def testAndSetSucceed() = {
    val rec = new Record("tassuc","tassuc1")
    connection.put("tass",rec)
    val ev = new ExistingValue("tassuc1",0)
    ev.unsetPrefix()
    rec.value = "tassuc2"
    val res = connection.test_and_set("tass",rec,ev)
    assert(res)
    val resp = connection.get("tass","tassuc")
    assert(resp.value === "tassuc2")
  }

  def testAndSetFail() = {
    val rec = new Record("tasfail","tasfail1")
    connection.put("tasf",rec)
    val ev = new ExistingValue("failval",0)
    ev.unsetPrefix()
    rec.value = "tasfail2"
    val res = 
      try {
        connection.test_and_set("tasf",rec,ev)
      } catch {
        case tsf: TestAndSetFailure => 
          {
            assert(tsf.currentValue === "tasfail1")
            true
          }
        case e: Exception => false
      }
    assert(res)
    val resp = connection.get("tasf","tasfail")
    assert(resp.value === "tasfail1")
  }

  def testAndSetSucceedPrefix() = {
    val rec = new Record("tassucp","tassucp1ignore")
    connection.put("tassp",rec)
    val ev = new ExistingValue("tassucp1xxx",7)
    rec.value = "tassucp2"
    val res = connection.test_and_set("tassp",rec,ev)
    assert(res)
    val resp = connection.get("tassp","tassucp")
    assert(resp.value === "tassucp2")
  }

  def testAndSetFailPrefix() = {
    val rec = new Record("tasfailp","tasfailp1ignore")
    connection.put("tasfp",rec)
    val ev = new ExistingValue("failval",12)
    rec.value = "tasfailp2"
    val res = 
      try {
        connection.test_and_set("tasfp",rec,ev)
      } catch {
        case tsf: TestAndSetFailure => 
          {
            assert(tsf.currentValue === "tasfailp1ignore")
            true
          }
        case e: Exception => false
      }
    assert(res)
    val resp = connection.get("tasfp","tasfailp")
    assert(resp.value === "tasfailp1ignore")
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

  def testGetSetAll() {
    KeyStoreUtils.putNumericKeys(0,100,"tgsa",connection)
    
    val targetSet = new RecordSet(RecordSetType.RST_ALL,
                                  null,null,null)
    val res: java.util.List[Record] = connection.get_set("tgsa",targetSet)
    assert(res.size() === 101)
    for (i <- 0 to 100) {
      val nk = new NumericKey(i)
      assert(res.get(i).value === nk.serialize)
    }
  }

  def testGetSetNone() {
    KeyStoreUtils.putNumericKeys(0,100,"tgsn",connection)
    
    val targetSet = new RecordSet(RecordSetType.RST_NONE,
                                  null,null,null)
    val res: java.util.List[Record] = connection.get_set("tgsn",targetSet)
    assert(res.size() === 0)
  }

  def testGetSetKeyFunc() {
    KeyStoreUtils.putNumericKeys(0,100,"tgskf",connection)
    val keyFunc = new UserFunction(Language.LANG_RUBY,
                               "Proc.new {|key| key.to_i%2==0}")
    val targetSet = new RecordSet(RecordSetType.RST_KEY_FUNC,
                                  null,keyFunc,null)
    val res: java.util.List[Record] = connection.get_set("tgskf",targetSet)
    assert(res.size() === 51)
    for (i <- 0 to 100 by 2) {
      val nk = new NumericKey(i)
      assert(res.get((i/2)).value == nk.serialize)
    }
  }

  def testConcurrentTestSet() {
	val firstRec = new Record("val", IntegerField(0).serialize)
	node.useConnection(_.put("concts", firstRec))

	val futures = (1 to 100).toList.map((i) => {
		future {
			node.useConnection((c) => {
				try {
					val oldRec = c.get("concts", "val")
					val oldVal = new IntegerField
					oldVal.deserialize(oldRec.value)
					val newValue = IntegerField(oldVal.value + 1)
					val ev = new ExistingValue()
					ev.setValue(oldRec.value)
					val newRec = new Record("val", newValue.serialize)
					c.test_and_set("concts", newRec, ev)
					1
				}
				catch {
					case e: TestAndSetFailure => 0
				}
			})
		}
	})
	val sum = futures.foldRight(0)((f: Future[Int], sum: Int) => {sum + f()})
	val finalRec = node.useConnection(_.get("concts", "val"))
	val finalVal = new IntegerField
	finalVal.deserialize(finalRec.value)

	assert(sum === finalVal.value)
  }
}

class BdbKeyStoreTest extends KeyStoreSuite {
  val node = new TestableBdbStorageNode()
  val connection = node.createConnection()
}

class SimpleKeyStoreTest extends KeyStoreSuite {
  val node = new TestableSimpleStorageNode()
  val connection = node.createConnection()
}
