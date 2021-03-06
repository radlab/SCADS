package edu.berkeley.cs.scads.comm

import java.nio._
import java.nio.channels._
import java.net.InetSocketAddress

import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.generic.IndexedRecord

case class RemoteNode(hostname:String, port: Int) {
    private val socketAddress = new InetSocketAddress(hostname, port)
    def getInetSocketAddress:InetSocketAddress = socketAddress
}

trait AvroChannelManager[SendMsgType <: IndexedRecord, RecvMsgType <: IndexedRecord] {
  /**
   * Send message to dest immediately.  Probably by calling sendMessageBulk and flush.
   * A new connection should be opened if one doesn't exist.
   */
  def sendMessage(dest: RemoteNode, msg: SendMsgType): Unit

  /**
   * Batch message for sending by serializing it into a buffer.
   * The message should be actually sent either when the buffer
   * is full or when flush is explicitly called.
   */
  def sendMessageBulk(dest: RemoteNode, msg: SendMsgType): Unit

  /**
   * Transmit any messages that are currently waiting in the sendbuffer.
   */
  def flush: Unit

  /**
   * Open a listening socket on the specified port
   */
  def startListener(port: Int): Unit

  /**
   * Abstract method to be implemented by the user
   */
  def receiveMessage(src: RemoteNode, msg: RecvMsgType):Unit
}

/*
class PrintAvroChannelManager[SendMsg <: SpecificRecord, RecvMsg <: SpecificRecord]
(implicit sendManifest: scala.reflect.Manifest[SendMsg],
recvManifest: scala.reflect.Manifest[RecvMsg])
extends NioAvroChannelManagerBase[SendMsg, RecvMsg] {
    override def receiveMessage(src: RemoteNode, msg: RecvMsg):Unit = {
        println("src: " + src)
        println("msg: " + msg)
    }
}

class EchoAvroChannelManager[EchoMsg <: SpecificRecord]
(implicit sendManifest: scala.reflect.Manifest[EchoMsg])
extends NioAvroChannelManagerBase[EchoMsg, EchoMsg] {
    override def receiveMessage(src: RemoteNode, msg: EchoMsg):Unit = {
        println("src: " + src)
        println("msg: " + msg)
        sendMessage(src, msg)
    }
}

class DiscardAvroChannelManager[SendMsg <: SpecificRecord, RecvMsg <: SpecificRecord]
(implicit sendManifest: scala.reflect.Manifest[SendMsg],
recvManifest: scala.reflect.Manifest[RecvMsg])
extends NioAvroChannelManagerBase[SendMsg, RecvMsg] {
    override def receiveMessage(src: RemoteNode, msg: RecvMsg):Unit = {
        // no op
    }
}
*/
