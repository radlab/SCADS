package edu.berkeley.cs.scads.storage
package transactions
package conflict

import actors.threadpool.ThreadPoolExecutor.AbortPolicy
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.ConcurrentHashMap
import java.util.Arrays

import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import java.io._
import org.apache.avro._
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecordBase, SpecificRecord}
import org.apache.avro.Schema

class ConflictResolver(val valueSchema: Schema, val ics: FieldICList) {
//  def getLUB(cstructs: Array[CStruct]): CStruct
//  def getGLB(cstructs: Array[CStruct]): CStruct

  def isCompatible(cstructs: Seq[CStruct]): Boolean = {
    if (cstructs.length <= 1) {
      true
    } else {
      val commandsList = cstructs.map(x => x.commands)
      // TODO: only pending?
      val head = reduceCommandList(commandsList.head.filter(_.pending))
      val tail = commandsList.tail
      tail.foldLeft[Boolean](true)(foldCommandList(head))
    }
  }

  private def compareCommandList(c1: Seq[Seq[CStructCommand]],
                                 c2: Seq[Seq[CStructCommand]]) = {
    val zipped = c1.zipAll(c2, List(), List())
    var success = true

    zipped.foreach(t => {
      if (success) {
        t match {
          case (l1: Seq[CStructCommand], l2: Seq[CStructCommand]) =>
            success = compareCommands(l1, l2)
          case (_, _) => success = false
        }
      }
    })
    success
  }

  private def compareCommands(c1: Seq[CStructCommand],
                              c2: Seq[CStructCommand]) = {
    if (c1.size != c2.size) {
      false
    } else {
      // Match xids for now.
      val map1 = c1.map(x => (x.xid, 1)).toMap
      var success = true
      c2.foreach(x => {
        if (!map1.contains(x.xid)) {
          success = false
        }
      })
      success
    }
  }

  // Reduces a sequence of logical updates into a single sequence.
  private def reduceCommandList(c: Seq[CStructCommand]) = {
    val reduced = new ListBuffer[ListBuffer[CStructCommand]]()
    var isLogical = false
    c.foreach(x => {
      x.command match {
        case up: LogicalUpdate => {
          if (isLogical) {
            reduced.last.append(x)
          } else {
            val newIds = new ListBuffer[CStructCommand]()
            newIds.append(x)
            reduced.append(newIds)
          }
          isLogical = true
        }
        case up: PhysicalUpdate => {
          val newIds = new ListBuffer[CStructCommand]()
          newIds.append(x)
          reduced.append(newIds)
          isLogical = false
        }
        case _ =>
      }
    })
    reduced
  }

  private def foldCommandList(head: Seq[Seq[CStructCommand]])(valid: Boolean, commands: Seq[CStructCommand]) = {
    if (!valid) {
      !valid
    } else {
      val pendingCommands = reduceCommandList(commands.filter(_.pending))
      compareCommandList(head, pendingCommands)
    }
  }
}
