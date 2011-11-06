package edu.berkeley.cs.scads.test.transactions
import org.scalatest.WordSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, WordSpec, OneInstancePerTest}
import org.scalatest.matchers.{HavePropertyMatchResult, HavePropertyMatcher, ShouldMatchers}

import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.storage.transactions.conflict._
import transactions.MDCCMetadata

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

//  // Returns a value record.
//  private def singleValueRecord(value: Int, version: Long) = {
//    val m = MDCCMetadata(version, List())
//    val v = ConflictValueRec(value.toString, value)
//    valueBuilder.toBytes(m, v)
//  }
//  // Returns single Version update list
//  private def singleVersionUpdate(key: Int, value: Int, version: Long) = {
//    val k = ConflictKeyRec(key, key.toString)
//    val m = MDCCMetadata(version, List())
//    val v = ConflictValueRec(value.toString, value)
//    VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
//  }
//  // Returns single Logical update list
//  private def singleLogicalUpdate(key: Int, value: Int, version: Long) = {
//    val k = ConflictKeyRec(key, key.toString)
//    val m = MDCCMetadata(version, List())
//    val v = ConflictValueRec(value.toString, value)
//    LogicalUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
//  }

//
//  // returns Version update lists
//  private def insertVersionUpdates(numKeys: Int) = {
//    0 until numKeys map (i => {
//      val k = ConflictKeyRec(i, i.toString)
//      val m = MDCCMetadata(0, List())
//      val v = ConflictValueRec(i.toString, i)
//      VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
//    })
//  }
//  // returns Value update lists
//  private def insertValueUpdates(numKeys: Int) = {
//    0 until numKeys map (i => {
//      val k = ConflictKeyRec(i, i.toString)
//      val m = MDCCMetadata(0, List())
//      val v = ConflictValueRec(i.toString, i)
//      ValueUpdate(keyBuilder.toBytes(k), None, valueBuilder.toBytes(m, v))
//    })
//  }
//  // Returns single Value update list
//  private def singleValueUpdate(key: Int, value: Int, oldValue: Option[Int]) = {
//    val k = ConflictKeyRec(key, key.toString)
//    // The version is not compared with ValueUpdate
//    val m = MDCCMetadata(0, List())
//    val v = ConflictValueRec(value.toString, value)
//    val oldV = oldValue.map(i => ConflictValueRec(i.toString, i))
//    List(ValueUpdate(keyBuilder.toBytes(k),
//                     oldV.map(valueBuilder.toBytes(m, _)),
//                     valueBuilder.toBytes(m, v)))
//  }

  // Shorthand for physical and logical updates.
  sealed trait UpdateType
  case class P(i: Int, pending: Boolean = true, commit: Boolean = true) extends UpdateType
  case class L(i: Int, pending: Boolean = true, commit: Boolean = true) extends UpdateType

  // Creates a simple cstruct with the specified update types.  Specific values
  // of the byte arrays are not created, because most test cases do not
  // consider them.
  private def cstruct(updates: Seq[UpdateType]): CStruct = {
    val commands = updates.map(_ match {
      case P(i, p, c) => CStructCommand(ScadsXid(i, i),
                                        VersionUpdate(null, null),
                                        p, c)
      case L(i, p, c) => CStructCommand(ScadsXid(i, i),
                                        LogicalUpdate(null, null),
                                        p, c)
    })
    CStruct(None, commands)
  }

  // Compares the sequence of commands for equivalent xids and flags.  Byte
  // arrays are not compared.
  private def simpleCompare(c1: CStruct, c2: CStruct): Boolean = {
    if (c1.commands.length != c2.commands.length) {
      false
    } else if (c1.commands.length == 0) {
      true
    } else {
      (c1.commands zip c2.commands).map(x => {
        val (a, b) = (x._1, x._2)
        a.xid == b.xid &&
        a.pending == b.pending &&
        a.commit == b.commit &&
        a.getClass.getName == b.getClass.getName
      }).reduce(_ && _)
    }
  }

  "A ConflictResolver" should {
    /***********************************************************************
     ******************************** GLB **********************************
     ********************************************************************* */
    "GLB of single cstruct" in {
      var golden = cstruct(P(1) :: L(2) :: P(3) :: P(4) :: L(5) :: Nil)
      var glb = resolver.getGLB(List(golden))
      simpleCompare(golden, glb) should be (true)
    }
    "GLB(1 command) pending and same command" in {
      var c1 = cstruct(P(1, true, true) :: Nil)
      var c2 = cstruct(P(1, true, true) :: Nil)
      var glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, c1) should be (true)

      c1 = cstruct(P(1, true, false) :: Nil)
      c2 = cstruct(P(1, true, false) :: Nil)
      glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, c1) should be (true)
    }
    "GLB(1 command) nonpending and same command" in {
      var c1 = cstruct(P(1, false, true) :: Nil)
      var c2 = cstruct(P(1, false, true) :: Nil)
      var glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, c1) should be (true)

      c1 = cstruct(P(1, false, false) :: Nil)
      c2 = cstruct(P(1, false, false) :: Nil)
      glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, c1) should be (true)
    }
    "GLB(1 command) nonpending overrides pending" in {
      var c1 = cstruct(P(1, true) :: Nil)
      var c2 = cstruct(P(1, false) :: Nil)
      var glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, cstruct(P(1, false) :: Nil)) should be (true)
    }
    "GLB(1 command) pending but conflicting commit" in {
      var c1 = cstruct(P(1, true, true) :: Nil)
      var c2 = cstruct(P(1, true, false) :: Nil)
      var glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, cstruct(Nil)) should be (true)
    }
    "GLB(1 command) different commands" in {
      var c1 = cstruct(P(1, true, true) :: Nil)
      var c2 = cstruct(P(2, true, true) :: Nil)
      var glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, cstruct(Nil)) should be (true)

      c1 = cstruct(P(1, false, false) :: Nil)
      c2 = cstruct(P(2, false, false) :: Nil)
      glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, cstruct(Nil)) should be (true)

      c1 = cstruct(P(1, true, false) :: Nil)
      c2 = cstruct(P(2, false, true) :: Nil)
      glb = resolver.getGLB(List(c1, c2))
      simpleCompare(glb, cstruct(Nil)) should be (true)
    }
    /***********************************************************************
     ******************************** LUB **********************************
     ********************************************************************* */
    "LUB of single cstruct" in {
      var golden = cstruct(P(1) :: L(2) :: P(3) :: P(4) :: L(5) :: Nil)
      var lub = resolver.getLUB(List(golden))
      simpleCompare(golden, lub) should be (true)
    }
    "LUB(1 command) pending and same command" in {
      var c1 = cstruct(P(1, true, true) :: Nil)
      var c2 = cstruct(P(1, true, true) :: Nil)
      var lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, c1) should be (true)

      c1 = cstruct(P(1, true, false) :: Nil)
      c2 = cstruct(P(1, true, false) :: Nil)
      lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, c1) should be (true)
    }
    "LUB(1 command) nonpending and same command" in {
      var c1 = cstruct(P(1, false, true) :: Nil)
      var c2 = cstruct(P(1, false, true) :: Nil)
      var lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, c1) should be (true)

      c1 = cstruct(P(1, false, false) :: Nil)
      c2 = cstruct(P(1, false, false) :: Nil)
      lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, c1) should be (true)
    }
    "LUB(1 command) nonpending overrides pending" in {
      var c1 = cstruct(P(1, true) :: Nil)
      var c2 = cstruct(P(1, false) :: Nil)
      var lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, cstruct(P(1, false) :: Nil)) should be (true)
    }
    "LUB(1 command) pending but conflicting commit" in {
      var c1 = cstruct(P(1, true, true) :: Nil)
      var c2 = cstruct(P(1, true, false) :: Nil)
      var lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, cstruct(Nil)) should be (true)
    }
    "LUB(1 command) different commands" in {
      var c1 = cstruct(P(1, true, true) :: Nil)
      var c2 = cstruct(P(2, true, true) :: Nil)
      var lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, cstruct(P(1, true, true) :: P(2, true, true) :: Nil)) should be (true)

      c1 = cstruct(P(1, false, false) :: Nil)
      c2 = cstruct(P(2, false, false) :: Nil)
      lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, cstruct(P(1, false, false) :: P(2, false, false) :: Nil)) should be (true)

      c1 = cstruct(P(1, true, false) :: Nil)
      c2 = cstruct(P(2, false, true) :: Nil)
      lub = resolver.getLUB(List(c1, c2))
      simpleCompare(lub, cstruct(P(1, true, false) :: P(2, false, true) :: Nil)) should be (true)
    }
  }
}
