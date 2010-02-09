package edu.berkeley.cs.scads.comm

import java.nio._
import java.nio.channels._
import java.net.InetSocketAddress

import org.apache.avro.io._
import org.apache.avro.ipc._
import org.apache.avro.specific._

import scala.collection.jcl.Conversions._

import edu.berkeley.cs.scads.Record

case class RemoteNode(hostname:String, port: Int) {
    private val socketAddress = new InetSocketAddress(hostname, port) 
    def getInetSocketAddress:InetSocketAddress = socketAddress 
}

trait AvroChannelManager[SendMsgType <: SpecificRecord, RecvMsgType <: SpecificRecord] {
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

class PrintAvroChannelManager extends NioAvroChannelManagerBase[Record, Record] {
    override def receiveMessage(src: RemoteNode, msg: Record):Unit = {
        println("src: " + src)
        println("msg: " + msg)
    }
}

class EchoAvroChannelManager extends NioAvroChannelManagerBase[Record, Record] {
    override def receiveMessage(src: RemoteNode, msg: Record):Unit = {
        println("src: " + src)
        println("msg: " + msg)
        sendMessage(src, msg)
    }
}
