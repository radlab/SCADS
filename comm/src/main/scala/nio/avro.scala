package edu.berkeley.cs.scads.comm

import java.net.{InetSocketAddress, InetAddress}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.{SocketChannel,NotYetConnectedException}

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor

import org.apache.avro.io._
import org.apache.avro.ipc._
import org.apache.avro.specific._

abstract class NioAvroChannelManagerBase[SendMsgType <: SpecificRecord,RecvMsgType <: SpecificRecord]
(implicit sendManifest: scala.reflect.Manifest[SendMsgType], 
recvManifest: scala.reflect.Manifest[RecvMsgType])
extends AvroChannelManager[SendMsgType, RecvMsgType] with ChannelHandler {

  private val msgRecvClass = recvManifest.erasure.asInstanceOf[Class[RecvMsgType]]
  private val msgSendClass = sendManifest.erasure.asInstanceOf[Class[SendMsgType]]

  private val msgReader = new SpecificDatumReader[RecvMsgType](msgRecvClass.newInstance.getSchema)
  private val msgWriter = new SpecificDatumWriter[SendMsgType](msgSendClass.newInstance.getSchema)

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

  private def getChannel(dest: RemoteNode):SocketChannel = {
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
    val encoder = new BinaryEncoder(buffer)
    msgWriter.write(msg, encoder)
    endpoint.sendBulk(channel, ByteBuffer.wrap(buffer.toByteArray), true)

  }

  override def sendMessage(dest: RemoteNode, msg: SendMsgType):Unit = {
    val channel = getChannel(dest)
    val buffer = new ByteArrayOutputStream(128)
    val encoder = new BinaryEncoder(buffer)
    msgWriter.write(msg, encoder)
    endpoint.send(channel, ByteBuffer.wrap(buffer.toByteArray), null, true)
  }

  override def flush: Unit = {
    endpoint.flushAllBulk
  }

  override def startListener(port: Int): Unit = {
    endpoint.serve(new InetSocketAddress(port))
  }

  override def processData(socket: SocketChannel, data: Array[Byte], count: Int) = {
    val inStream = new BinaryDecoder(new ByteBufferInputStream(java.util.Arrays.asList(ByteBuffer.wrap(data))))
    val msg = msgRecvClass.newInstance
    msgReader.read(msg, inStream)
    receiveMessage(socketAddrReverseMap.get(socket), msg)
  }

  override def handleException(exception: Exception) = {
    //TODO: do something better
    exception.printStackTrace
  }

  def getLocalAddress: InetAddress = endpoint.getListeningAddr
  def getLocalPort: Int = endpoint.getListeningPort
    
}
