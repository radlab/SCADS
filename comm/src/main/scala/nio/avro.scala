package edu.berkeley.cs.scads.comm

import java.net.InetSocketAddress

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.{SocketChannel,NotYetConnectedException}

import java.util.concurrent.Executor

import org.apache.avro.io._
import org.apache.avro.ipc._
import org.apache.avro.specific._

abstract class NioAvroChannelManagerBase[SendMsgType <: SpecificRecord,RecvMsgType <: SpecificRecord]
    (private val readExecutor: Executor)
    (implicit sendManifest: scala.reflect.Manifest[SendMsgType], 
     recvManifest: scala.reflect.Manifest[RecvMsgType])
    extends AvroChannelManager[SendMsgType, RecvMsgType] with ChannelHandler {

    private val msgReader = new SpecificDatumReader(recvManifest.erasure.asInstanceOf[Class[RecvMsgType]])
	private val msgWriter = new SpecificDatumWriter(sendManifest.erasure.asInstanceOf[Class[SendMsgType]])
	private val msgRecvClass = recvManifest.erasure.asInstanceOf[Class[RecvMsgType]]

    protected val endpoint: AbstractNioEndpoint
    
	override def sendMessage(dest: RemoteNode, msg: SendMsgType):Unit = {
        val channel = endpoint.getChannelForInetSocketAddress(dest.getInetSocketAddress)
        if (channel == null)
            throw new NotYetConnectedException
		val stream = new ByteArrayOutputStream(8192)
		val enc = new BinaryEncoder(stream)
        msgWriter.write(msg.asInstanceOf[Object], enc)
        endpoint.send(channel, stream.toByteArray, null, true)
    }

    override def processData(socket: SocketChannel, data: Array[Byte], count: Int) = {
        val inStream = new BinaryDecoder(new ByteBufferInputStream(java.util.Arrays.asList(ByteBuffer.wrap(data))))
        val msg = msgRecvClass.newInstance
        msgReader.read(msg.asInstanceOf[Object], inStream)
        receiveMessage(new RemoteNode(socket.socket.getInetAddress.getHostName, socket.socket.getPort), msg)
    }

    override def handleException(exception: Exception) = {
        //TODO: do something better
        exception.printStackTrace
    }
}

abstract class NioAvroServer[SendMsgType <: SpecificRecord,RecvMsgType <: SpecificRecord]
    (private val hostAddr: InetSocketAddress,
     readExecutor: Executor)
    (implicit sendManifest: scala.reflect.Manifest[SendMsgType], 
     recvManifest: scala.reflect.Manifest[RecvMsgType])
    extends NioAvroChannelManagerBase[SendMsgType,RecvMsgType](readExecutor)(sendManifest,recvManifest) {

    override protected val endpoint = new NioServer(hostAddr, readExecutor, this)

    def serve = endpoint.serve
}


abstract class NioAvroClient[SendMsgType <: SpecificRecord,RecvMsgType <: SpecificRecord]
    (readExecutor: Executor)
    (implicit sendManifest: scala.reflect.Manifest[SendMsgType], 
     recvManifest: scala.reflect.Manifest[RecvMsgType])
    extends NioAvroChannelManagerBase[SendMsgType,RecvMsgType](readExecutor)(sendManifest,recvManifest) {

    override protected val endpoint = new NioClient(readExecutor, this)

    def connect(hostAddress: InetSocketAddress):ConnectFuture = endpoint.asInstanceOf[NioClient].connect(hostAddress)
}
