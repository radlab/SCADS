package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._
import edu.berkeley.cs.scads.comm.Storage.AvroConversions._

import edu.berkeley.cs.scads.storage._

import org.apache.log4j.Logger
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer

object KeyStoreSpec extends SpecificationWithJUnit("KeyStore Specification") {
  private val lgr = Logger.getLogger("KeyStoreSpec")

  val intRec = new IntRec
  val strRec = new StringRec
  println("Creating namespace for int")
  if (!TestScalaEngine.cluster.createNamespaceIfNotExists("KeySpecInt", intRec.getSchema(), intRec.getSchema()))
    println("Already there")
  println("Creating namespace for string")
  if (!TestScalaEngine.cluster.createNamespaceIfNotExists("KeySpecString", strRec.getSchema(), strRec.getSchema()))
    println("Already there")

  val cr = new ConfigureRequest
  cr.namespace = "KeySpecInt"
  cr.partition = "1"
  println("Making CR request: " + cr)
  Sync.makeRequest(TestScalaEngine.node,  new Utf8("Storage"), cr)

  val cr2 = new ConfigureRequest
  cr2.namespace = "KeySpecString"
  cr2.partition = "1"
  println("Making CR2 request: " + cr2)
  Sync.makeRequest(TestScalaEngine.node,  new Utf8("Storage"), cr2)

  private def mustBeFalse(b: Boolean, msg: String) = mustBeTrue(!b, msg)

  private def mustBeFalse(b: Boolean):Unit = mustBeTrue(!b)

  private def mustBeTrue(b: Boolean):Unit = mustBeTrue(b, "Default fail")

  private def mustBeTrue(b: Boolean, msg: String):Unit = {
    if(!b) fail(msg)
    b must_==true
  }

  "a keystore" should {
    "store key/value pairs such that" >> {
      "updates persist" in {
        lgr.warn("test 1 begin")
        putIntKV("KeySpecInt", 1, 1)
        val resp = getIntV("KeySpecInt", 1)
        mustBeTrue(resp.equals(GetResponse(1, 1)), "get not the same as put")
        lgr.warn("test 1 end")
      }

      "updates change the value" in {
        lgr.warn("test 2 begin")
        putIntKV("KeySpecInt", 2, 2)
        val resp = getIntV("KeySpecInt", 2)
        mustBeTrue(resp.equals(GetResponse(2, 2)), "get not same as put")

        putIntKV("KeySpecInt", 2, 10)
        val update = getIntV("KeySpecInt", 2)
        mustBeTrue(update.equals(GetResponse(2, 10)), "value not changed on update")

        putIntKV("KeySpecInt", 2, 1000)
        val update2 = getIntV("KeySpecInt", 2)
        mustBeTrue(update2.equals(GetResponse(2, 1000)), "value not changed on update")
        lgr.warn("test 2 end")
      }

      "keys with null values are deleted" in {
        putIntKV("KeySpecInt", 3, 1000)
        val update = getIntV("KeySpecInt", 3)
        mustBeTrue(update.equals(GetResponse(3, 1000)))
        putIntKNullV("KeySpecInt", 3)
        mustBeTrue(getIntVNull("KeySpecInt", 3), "null value did not delete key")
        lgr.info("end test 3")
      }

      "succedes when unchanged" in {
        lgr.info("begin tset 1")
        putIntKV("KeySpecInt", 4, 100)
        mustBeTrue(getIntV("KeySpecInt",4).equals(GetResponse(4,100)),
                   "previous put failed")
        mustBeTrue(testSetKV("KeySpecInt", 4, 101, 100, false), 
                   "TSET did not succeed even though expected value was current")
        lgr.info("end tset 1")
      }

      "fails when the value has changed" in {
        putIntKV("KeySpecInt", 5, 100)
        mustBeFalse(testSetKV("KeySpecInt", 5, 101, 101, false),
                    "TSET didn't fail even when expected value changed")
      }

      "succeds when null is expected" in {
        // delete the key in case it's persisted
        putIntKNullV("KeySpecInt",6)
        mustBeTrue(testSetKVExpectNull("KeySpecInt", 6, 1),
                   "TSET didn't succeed when null expected, and no prev mapping")
      }

      "fails when null isn't there" in {
        putIntKV("KeySpecInt", 7, 1000)
        mustBeFalse(testSetKVExpectNull("KeySpecInt", 7, 1),
                    "TSET didn't fail when null expected, and prev mapping exists")
      }

      /*
      "succeeds when a prefix is unchanged" in {
        lgr.warn("prefix test 1 begin")
      putStringKV("KeySpecString", "key1", "value1ignoreme")
      val res1 = getStringV("KeySpecString", "key1")
      lgr.warn("res1: " + res1) 
      mustBeTrue(res1.equals(StringGetResponse("key1","value1ignoreme")),
      "get did not equal put record")
      mustBeTrue(testSetKVStr("KeySpecString", "key1", "valueCHANGED", "value1", true),
      "TSET failed, even when exp value was correct w/ prefix")
      val res2 = getStringV("KeySpecString", "key1")
      mustBeTrue(res2.equals(StringGetResponse("key1","valueCHANGED")),
      "TSET mapping did not persist")
      lgr.warn("res2: " + res2)
      lgr.warn("prefix test 1 end")
      }
      */
      /*
      Here's why this doesn't work.
      scala> val a = new StringRec
      scala> a.f1 = new Utf8("abcd")
      scala> val b = new StringRec
      scala> b.f1 = new Utf8("abcdefgh")
      scala> a.toBytes
      a.toBytes
      res4: Array[Byte] = Array(8, 97, 98, 99, 100)

      scala> b.toBytes
      b.toBytes
      res5: Array[Byte] = Array(16, 97, 98, 99, 100, 101, 102, 103, 104)
      because of the length prefix header, prefix comparisons like this
      just will not work
      */

      "get range sets works" in {
        (33 to 37).foreach(i => putIntKV("KeySpecInt", i, i))
        val recordSet = getIntRangeV("KeySpecInt",33,38)
        lgr.warn("recordSet received: " + recordSet)
        val expc = (33 to 37).map(i => GetResponse(i, i))
        val actual = makeIntRecords(recordSet) 
        println("expc: " + expc)
        println("actual: " + actual)
        expc must containInOrder(actual)
      }

    }
  }

  def makeIntRecords(recordSet: RecordSet):List[GetResponse] = {
    var rtn = List[GetResponse]()
    val iter = recordSet.records.iterator
    while (iter.hasNext) {
      val rec = iter.next
      rtn = rtn ::: List[GetResponse](rec)
    }
    rtn
  }

  implicit def rec2GetResp(rec: Record):GetResponse = {
    val k2 = new IntRec
    val v2 = new IntRec
    k2.parse(rec.key)
    v2.parse(rec.value)
    GetResponse(k2.f1, v2.f1)
  }

  implicit def int2IntRec(int: Int):IntRec = {
    val r = new IntRec
    r.f1 = int
    r
  }

  implicit def string2StringRec(str: String):StringRec = {
    if (str == null) return null
    val r = new StringRec
    r.f1 = str
    r
  }

  implicit def intTuple2PutResp(tuple: (Int,Int)): GetResponse = {
    GetResponse(tuple._1, tuple._2)
  }

  case class StringGetResponse(val key: String, val value: String)

  case class GetResponse(val key: Int, val value: Int)

  private def putIntKNullV(ns: String, key: Int) = {
    val k1 = new IntRec
    k1.f1 = key
    putKVBytes(ns, k1.toBytes, null)
  }

  private def putStringKV(ns: String, key: String, value: String) = {
    val k1 = new StringRec
    val v1 = new StringRec
    k1.f1 = key
    v1.f1 = value
    putKVBytes(ns, k1.toBytes, v1.toBytes)
  }

  private def putIntKV(ns: String, key: Int, value: Int) = {
    val k1 = new IntRec
    val v1 = new IntRec
    k1.f1 = key 
    v1.f1 = value 
    putKVBytes(ns, k1.toBytes, v1.toBytes)
  }

  /** Writes to wire */
  private def putKVBytes(ns: String, key: Array[Byte], value: Array[Byte]) = {
    val pr = new PutRequest
    pr.namespace = ns 
    pr.key = key
    pr.value = value match {
        case null => null
        case _    => ByteBuffer.wrap(value)
    }
    Sync.makeRequest(TestScalaEngine.node,  new Utf8("Storage"), pr)
  }

  private def getBytesV(ns:String, key: Array[Byte]):Record = {
    val gr = new GetRequest
    gr.namespace = ns
    gr.key = key
    Sync.makeRequest(TestScalaEngine.node,  new Utf8("Storage"), gr).asInstanceOf[Record]
  }

  private def getStringV(ns: String, key:String):StringGetResponse = {
    val res = getBytesV(ns, string2StringRec(key).toBytes)
    val k2 = new StringRec
    val v2 = new StringRec
    k2.parse(res.key)
    v2.parse(res.value)
    StringGetResponse(k2.f1, v2.f1)
  }

  private def getAllIntRangeV(ns: String): RecordSet = getIntRangeV(ns, 0, 0, true)

  private def getIntRangeV(ns: String, start: Int, end: Int):RecordSet = getIntRangeV(ns, start, end, false)

  private def getIntRangeV(ns: String, start: Int, end: Int, all: Boolean):RecordSet = {
    val rr = new GetRangeRequest
    rr.namespace = ns
    val kr = new KeyRange
    rr.range = kr
    kr.limit = null
    kr.offset = null
    kr.backwards = false
    if (!all) {
      kr.minKey = ByteBuffer.wrap(start.toBytes)
      kr.maxKey = ByteBuffer.wrap(end.toBytes)
    } else {
      kr.minKey = null
      kr.maxKey = null
    }
    Sync.makeRequest(TestScalaEngine.node,  new Utf8("Storage"), rr).asInstanceOf[RecordSet]
  }

  /** Blocks for result */
  private def getIntV(ns: String, key: Int):GetResponse = {
    val keyRec = new IntRec
    keyRec.f1 = key
    val res = getBytesV(ns, keyRec.toBytes)
    res
  }

  private def getIntVNull(ns:String, key:Int):Boolean = {
    val res = getBytesV(ns, key.toBytes)
    res == null
  }

  /* True if succeeds, false otherwise */
  private def testSetKV(ns: String, key: Int, value: Int, expected: Int, prefix: Boolean):Boolean = testSetKV(ns, key, value, expected, prefix, false)

  private def testSetKVExpectNull(ns: String, key: Int, value: Int): Boolean = testSetKV(ns, key, value, 0, false, true)

  private def testSetKV(ns: String, key: Int, value: Int, expected: Int, prefix: Boolean, nullExp: Boolean):Boolean = {
    val keyRec: IntRec = key
    val valRec: IntRec = value
    val expRec: IntRec = expected
    val expVal = nullExp match {
      case true => null
      case false  => expRec.toBytes
    }
    testSetKVBytes(ns, keyRec.toBytes, valRec.toBytes, expVal, prefix)
  }

  private def testSetKVStr(ns: String, key: String, value: String, expected: String, prefix: Boolean):Boolean = {
    val keyRec: StringRec = key
    val valRec: StringRec = value
    val expRec: StringRec = expected
    testSetKVBytes(ns, keyRec.toBytes, valRec.toBytes, expRec.toBytes, prefix)
  }

  private def testSetKVBytes(ns: String, key: Array[Byte], value: Array[Byte], expected: Array[Byte], prefix: Boolean):Boolean = {
    val req = new TestSetRequest
    req.namespace = ns
    req.key = key
    req.value = value
    req.expectedValue = expected match {
        case null => null
        case _    => ByteBuffer.wrap(expected)
    }
    req.prefixMatch = prefix
    println("Writing TSET request: ")
    val res = Sync.makeRequest(TestScalaEngine.node,  new Utf8("Storage"), req)
    if (res!=null) lgr.warn("test and set failed: " + res)
    res == null
  }
}

class KeyStoreTest extends JUnit4(KeyStoreSpec)
