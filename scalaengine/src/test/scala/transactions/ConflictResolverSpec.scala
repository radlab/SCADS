package edu.berkeley.cs.scads.test.transactions
import org.scalatest.WordSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, WordSpec, OneInstancePerTest}
import org.scalatest.matchers.{HavePropertyMatchResult, HavePropertyMatcher, ShouldMatchers}

import java.io.File
import java.lang.{ Integer => JInteger }
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

import org.apache.avro._
import specific._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage.transactions.conflict._

case class ConflictKeyRec(var i: Int, var s: String) extends AvroRecord
case class ConflictValueRec(var s: String, var i: Int) extends AvroRecord

@RunWith(classOf[JUnitRunner])
class ConflictResolverSpec extends WordSpec
with ShouldMatchers
with BeforeAndAfterAll
with BeforeAndAfterEach {
  private val keyBuilder = new KeyBuilder[ConflictKeyRec]
  private val valueBuilder = new ValueBuilder[ConflictValueRec]

  private val keySchema = ConflictKeyRec(1,"1").getSchema
  private val valueSchema = ConflictValueRec("1", 1).getSchema

  private val resolver = new ConflictResolver(valueSchema, FieldICList(List()))

  // Returns a value record.
  private def singleValueRecord(value: Int, version: Long) = {
    val m = MDCCMetadata(version, List())
    val v = ConflictValueRec(value.toString, value)
    valueBuilder.toBytes(m, v)
  }

  // returns Version update lists
  private def insertVersionUpdates(numKeys: Int) = {
    0 until numKeys map (i => {
      val k = ConflictKeyRec(i, i.toString)
      val m = MDCCMetadata(0, List())
      val v = ConflictValueRec(i.toString, i)
      VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
    })
  }
  // Returns single Version update list
  private def singleVersionUpdate(key: Int, value: Int, version: Long) = {
    val k = ConflictKeyRec(key, key.toString)
    val m = MDCCMetadata(version, List())
    val v = ConflictValueRec(value.toString, value)
    VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
  }
  // returns Value update lists
  private def insertValueUpdates(numKeys: Int) = {
    0 until numKeys map (i => {
      val k = ConflictKeyRec(i, i.toString)
      val m = MDCCMetadata(0, List())
      val v = ConflictValueRec(i.toString, i)
      ValueUpdate(keyBuilder.toBytes(k), None, valueBuilder.toBytes(m, v))
    })
  }
  // Returns single Value update list
  private def singleValueUpdate(key: Int, value: Int, oldValue: Option[Int]) = {
    val k = ConflictKeyRec(key, key.toString)
    // The version is not compared with ValueUpdate
    val m = MDCCMetadata(0, List())
    val v = ConflictValueRec(value.toString, value)
    val oldV = oldValue.map(i => ConflictValueRec(i.toString, i))
    List(ValueUpdate(keyBuilder.toBytes(k),
                     oldV.map(valueBuilder.toBytes(m, _)),
                     valueBuilder.toBytes(m, v)))
  }

  "A ConflictResolver" should {
    "be compatible for no cstructs" in {
      resolver.isCompatible(List()) should be (true)
    }

    "be compatible for a single empty cstruct" in {
      val c1 = CStruct(Option(singleValueRecord(1, 1)), List())
      resolver.isCompatible(List(c1)) should be (true)
    }

    "be compatible for a single cstruct" in {
      val c1 = CStruct(Option(singleValueRecord(1, 1)),
                       List(CStructCommand(ScadsXid(1, 1),
                                           singleVersionUpdate(1, 1, 2),
                                           true)))
      resolver.isCompatible(List(c1)) should be (true)
    }

    "be compatible for two identical cstructs" in {
      val c1 = CStruct(Option(singleValueRecord(1, 1)),
                       List(CStructCommand(ScadsXid(1, 1),
                                           singleVersionUpdate(1, 1, 2),
                                           true)))
      resolver.isCompatible(List(c1, c1)) should be (true)
    }

    "be incompatible for two different cstructs" in {
      val c1 = CStruct(Option(singleValueRecord(1, 1)),
                       List(CStructCommand(ScadsXid(1, 1),
                                           singleVersionUpdate(1, 1, 2),
                                           true)))
      val c2 = CStruct(Option(singleValueRecord(1, 1)),
                       List(CStructCommand(ScadsXid(2, 2),
                                           singleVersionUpdate(1, 2, 2),
                                           true)))
      resolver.isCompatible(List(c1, c2)) should be (false)
    }
  }
}
