package edu.berkeley.cs
package scads
package comm

import config._

import java.nio._
import java.nio.channels._
import java.net.InetSocketAddress

import avro.marker._
import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.generic.IndexedRecord
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import net.lag.logging.Logger
import javax.swing.UIDefaults.LazyInputMap

case class RemoteNode(var hostname:String, var port: Int) extends AvroRecord {
    def socketAddress = new InetSocketAddress(hostname, port)

    @deprecated("use socketAddress", "v2.1.3")
    def getInetSocketAddress:InetSocketAddress = socketAddress
}

trait AvroChannelManager[SendMsgType <: IndexedRecord, RecvMsgType <: IndexedRecord] {
  private val logger = Logger()

  val hostname =
    if(System.getProperty("scads.comm.externalip") == null) {
      logger.debug("Using ip address from java.net.InetAddress.getLocalHost")
      java.net.InetAddress.getLocalHost.getCanonicalHostName()
    }
    else {
      val httpClient = new HttpClient()
      val getMethod = new GetMethod("http://instance-data/latest/meta-data/public-hostname")
      httpClient.executeMethod(getMethod)
      val externalIP = getMethod.getResponseBodyAsString
      logger.info("Using external ip address on EC2: %s", externalIP)
      externalIP
    }

  val port: Int = initListener()

  val remoteNode = RemoteNode(hostname, port)

  /**Naively increments port until a valid one is found */
  private def initListener() = {
    var port = Config.config.getInt("scads.comm.listen", 9000)
    var numTries = 0
    var found = false
    while (!found && numTries < 500) {
      try {
        startListener(port)
        found = true
      } catch {
        case ex: org.jboss.netty.channel.ChannelException =>
          logger.debug("Could not listen on port %d, trying %d".format(port, port + 1))
          port += 1
      } finally {
        numTries += 1
      }
    }
    if (found)
      port
    else throw new RuntimeException("Could not initialize listening port in 50 tries")
  }

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
  protected def startListener(port: Int): Unit

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
