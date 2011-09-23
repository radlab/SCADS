package edu.berkeley.cs.scads.storage.transactions.conflict

import edu.berkeley.cs.scads.comm._

import edu.berkeley.cs.scads.storage.MDCCRecordUtil
import edu.berkeley.cs.scads.storage.transactions._

import actors.threadpool.ThreadPoolExecutor.AbortPolicy
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.ConcurrentHashMap
import java.util.Arrays

import scala.collection.mutable.Buffer
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
      val head = commandsList.head
      val tail = commandsList.tail
      tail.foldLeft[Boolean](true)(foldCommands(head))
    }
  }

  private def compareCommands(c1: Seq[CStructCommand],
                              c2: Seq[CStructCommand]) = {
    val zipped = c1.zipAll(c2, null, null)
    var success = true

    // TODO: take care of more complicated situations, like logical updates,
    //       and multiple physical updates.
    zipped.foreach(t => {
      if (success) {
        t match {
          case (CStructCommand(id1, up1: PhysicalUpdate, _),
                CStructCommand(id2, up2: PhysicalUpdate, _)) =>
                  success = id1 == id2
          case (CStructCommand(id1, up1: LogicalUpdate, _),
                CStructCommand(id2, up2: LogicalUpdate, _)) =>
                  success = true
          case (_, _) => success = false
        }
      }
    })
    success
  }

  private def foldCommands(head: Seq[CStructCommand])(valid: Boolean, commands: Seq[CStructCommand]) = {
    if (!valid) {
      !valid
    } else {
      val pendingHead = head.filter(_.pending)
      val pendingCommands = commands.filter(_.pending)
      compareCommands(pendingHead, pendingCommands)
    }
  }
}
