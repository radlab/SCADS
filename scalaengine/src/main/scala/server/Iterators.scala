package edu.berkeley.cs.scads.storage

import scala.actors._
import scala.actors.Actor._

import org.apache.log4j.Logger

import org.apache.avro.generic.GenericData.{Array => AvroArray}

import com.sleepycat.je.DatabaseEntry

import edu.berkeley.cs.scads.comm._

class RecvIter(id: ActorId, logger:Logger) {
  def doRecv(recFunc:(Record) => Unit, finFunc:() => Unit) {
    loop {
      react {
        case (rn:RemoteNode, msg: Message) => msg.body match {
          case bd:BulkData => {
            logger.debug("Got bulk data, inserting")
            val it = bd.records.records.iterator
            while(it.hasNext) {
              val rec = it.next
              recFunc(rec)
            }
            val bda = new BulkDataAck
            bda.seqNum = bd.seqNum
            bda.sendActorId = id
            val msg = new Message
            msg.body = bda
            //msg.dest = new java.lang.Long(bd.sendActorId)
            msg.dest = bd.sendActorId
            msg.src = id
            MessageHandler.sendMessage(rn,msg)
            logger.debug("Done and acked")
          }
          case tf:TransferFinished => {
            logger.debug("Got copy finished")
            finFunc()
            val ric = new RecvIterClose
            ric.sendActorId = id
            val msg = new Message
            msg.body = ric
            //msg.dest = new java.lang.Long(tf.sendActorId)
            msg.dest = tf.sendActorId
            msg.src = id
            MessageHandler.sendMessage(rn,msg)
            exit()
          }
          case m => {
            logger.warn("RecvIter got unexpected type of message: "+msg.body)
          }
        }
        case msg => {
          logger.warn("RecvIter got unexpected message: "+msg)
        }
      }
    }
  }
}

class RecvPullIter(id: ActorId, logger:Logger) extends Iterator[Record] {
  private var curRecs: Iterator[Record] = null
  private var done = false

  private def nextBuf() = {
    if (!done) {
      receive {
        case (rn:RemoteNode, msg: Message) => msg.body match {
          case bd:BulkData => {
            curRecs = bd.records.records.iterator
            val bda = new BulkDataAck
            bda.seqNum = bd.seqNum
            bda.sendActorId = id
            val msg = new Message
            msg.body = bda
            //msg.dest = new java.lang.Long(bd.sendActorId)
            msg.dest = bd.sendActorId
            msg.src = id
            MessageHandler.sendMessage(rn,msg)
          }
          case ric:TransferFinished => {
            logger.debug("RecvPullIter: Got copy finished")
            done = true
            curRecs = null
          }
          case m => {
            logger.warn("RecvPullIter: got unexpected type of message: "+msg.body)
            done = true
            curRecs = null
          }
        }
        case msg => {
          logger.warn("RecvPullIter: got unexpected message: "+msg)
          done = true
          curRecs = null
        }
      }
    }
  }

  def hasNext(): Boolean = {
    if (done) return false
    if (curRecs == null || !(curRecs.hasNext)) nextBuf
    if (done) return false
    return true
  }

  def next(): Record = {
    if (done) return null
    if (curRecs == null || !(curRecs.hasNext)) nextBuf
    if (done) return null
    curRecs.next
  }
}

class SendIter(targetNode:RemoteNode, id: ActorId, receiverId: ActorId, capacity:Int, speedLimit:Int, logger:Logger) {
  private var windowLeft = 50 // we'll allow 50 un-acked bulk messages for now
  private var seqNum = 0
  private var bytesQueued:Long = 0
  private var bytesSentLast:Long = 0
  private var lastSentTime:Long = 0
  private var buffer = new scala.collection.mutable.ArrayBuffer[Record](capacity + 1)

  def flush() {
    if (speedLimit != 0) {
      val secs = (System.currentTimeMillis - lastSentTime)/1000.0
      val target = bytesSentLast / speedLimit.toDouble
      if (target > secs)  // if we wanted to take longer than we actually did
        Thread.sleep(((target-secs)*1000).toLong)
    }
    val bd = new BulkData
    bd.seqNum = seqNum
    seqNum += 1
    bd.sendActorId = id
    val rs = new RecordSet
    rs.records = buffer.toList
    bd.records = rs
    val msg = new Message
    msg.dest = receiverId
    msg.src = id
    msg.body = bd
    MessageHandler.sendMessage(targetNode,msg)
    windowLeft -= 1
    buffer.clear
    if (speedLimit != 0) {
      bytesSentLast = bytesQueued
      bytesQueued = 0
      lastSentTime = System.currentTimeMillis()
    }
  }

  def finish(timeout:Int): Unit = {
    flush
    val fin = new TransferFinished
    fin.sendActorId = id
    val msg = new Message
    msg.src = id
    msg.dest = receiverId
    msg.body = fin
    MessageHandler.sendMessage(targetNode,msg)
    // now we loop and wait for acks and final cleanup from other side
    var done = false
    while(!done) {
      receiveWithin(timeout) {
        case (rn:RemoteNode, msg:Message)  => msg.body match {
          case bda:BulkDataAck => {
            // don't need to do anything, we're ignoring these
          }
          case ric:RecvIterClose => { // should probably have a status here at some point
            done = true
          }
          case msgb => {
            logger.warn("Copy end loop got unexpected message body: "+msgb)
          }
        }
          case TIMEOUT => {
            logger.warn("Copy end loop timed out waiting for finish")
            done = true
          }
        case msg =>
          logger.warn("Copy end loop got unexpected message: "+msg)
      }
    }
  }

  def put(rec:Record): Unit = {
    while (windowLeft <= 0) { // not enough acks processed
      receiveWithin(60000) { // we'll allow 1 minute for an ack
        case (rn:RemoteNode, msg:Message)  => msg.body match {
          case bda:BulkDataAck => {
            logger.debug("Got ack, increasing window")
            windowLeft += 1
          }
          case msgb =>
            logger.warn("SendIter got unexpected message body: "+msgb)
        }
        case TIMEOUT => {
          logger.warn("SendIter timed out waiting for ack")
          exit()
        }
        case msg =>
          logger.warn("SendIter got unexpected message: "+msg)
      }
    }
    buffer += rec
    bytesQueued += rec.value.size
    if (buffer.size >= capacity)  // time to send
      flush
  }
}

