package edu.berkeley.cs
package scads.comm

import java.net.{InetSocketAddress, InetAddress}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.{SocketChannel, NotYetConnectedException}

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor

import org.apache.avro.io._
import org.apache.avro.generic._
import org.apache.avro.specific._
import avro.runtime._
import net.lag.logging.Logger

import avro.runtime.TypedSchema

/**
 * Easier to instantiate via reflection
 */
class DefaultNioChannelManager[S <: IndexedRecord, R <: IndexedRecord](
                                                                        recvMsg: (AvroChannelManager[S, R], RemoteNode, R) => Unit, sendSchema: TypedSchema[S], recvSchema: TypedSchema[R])
  extends NioAvroChannelManagerBase[S, R]()(sendSchema, recvSchema) {

  override def receiveMessage(remoteNode: RemoteNode, msg: R) {
    recvMsg(this, remoteNode, msg)
  }
}

abstract class NioAvroChannelManagerBase[SendMsgType <: IndexedRecord, RecvMsgType <: IndexedRecord]
(implicit sendSchema: TypedSchema[SendMsgType], recvSchema: TypedSchema[RecvMsgType])
  extends AvroChannelManager[SendMsgType, RecvMsgType] with ChannelHandler {

  protected val logger: Logger = Logger()

  private val msgReader = new SpecificDatumReader[RecvMsgType](recvSchema)
  private val msgWriter = new SpecificDatumWriter[SendMsgType](sendSchema)

  protected val endpoint: NioEndpoint = new NioEndpoint(this)
  endpoint.acceptEventHandler = new NioAcceptEventHandler {
    override def acceptEvent(channel: SocketChannel) = {
      registerReverseMap(channel)
    }
  }
  endpoint.connectEventHandler = new NioConnectEventHandler {
    override def connectEvent(channel: SocketChannel) = {
      registerReverseMap(channel)
    }
  }

  private val socketAddrReverseMap = new ConcurrentHashMap[SocketChannel, RemoteNode]

  private def registerReverseMap(channel: SocketChannel) = {
    socketAddrReverseMap.put(
      channel,
      RemoteNode(channel.socket.getInetAddress.getHostName, channel.socket.getPort))
  }

  private def getChannel(dest: RemoteNode): SocketChannel = {
    val sockAddr = dest.getInetSocketAddress
    val channel = endpoint.getChannelForInetSocketAddress(sockAddr)
    if (channel != null) return channel
    val future = endpoint.connect(sockAddr)
    future.await
    future.clientSocket
  }

  override def sendMessageBulk(dest: RemoteNode, msg: SendMsgType): Unit = {
    val channel = getChannel(dest)
    val buffer = new ByteArrayOutputStream(128)
    val encoder = EncoderFactory.get().binaryEncoder(buffer, null)
    msgWriter.write(msg, encoder)
    encoder.flush
    endpoint.sendBulk(channel, ByteBuffer.wrap(buffer.toByteArray), true)

  }

  override def sendMessage(dest: RemoteNode, msg: SendMsgType): Unit = {
    val channel = getChannel(dest)
    val buffer = new ByteArrayOutputStream(128)
    val encoder = EncoderFactory.get().binaryEncoder(buffer, null)
    msgWriter.write(msg, encoder)
    encoder.flush
    endpoint.send(channel, ByteBuffer.wrap(buffer.toByteArray), null, true)
  }

  override def flush: Unit = {
    endpoint.flushAllBulk
  }

  def startListener(): Unit = {
    var port = 9000
    synchronized {
      var open = false
      while (!open) {
        try {
          startListener(port)
          open = true
        } catch {
          case bn: java.net.BindException => port += 1
        }
      }
    }
  }

  override def startListener(port: Int): Unit = {
    endpoint.serve(new InetSocketAddress(port))
    logger.info("Listener started on port: %d", port)
  }

  override def processData(socket: SocketChannel, data: Array[Byte], count: Int) = {
    //TODO: consider using direct binary decoders, since there's no reason to
    //buffer (saves a copy of the data)
    val inStream = DecoderFactory.get().binaryDecoder(data, null)
    val msg = msgReader.read(null.asInstanceOf[RecvMsgType], inStream)
    receiveMessage(socketAddrReverseMap.get(socket), msg)
  }

  override def handleException(exception: Exception) = {
    //TODO: do something better
    exception.printStackTrace
  }

  def getLocalAddress: InetAddress = endpoint.getListeningAddr

  def getLocalPort: Int = endpoint.getListeningPort

  def remoteNode = {
    RemoteNode(getLocalAddress.getCanonicalHostName(), getLocalPort)
  }
}
