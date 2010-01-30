package edu.berkeley.cs.scads.comm

import java.nio._
import java.nio.channels._
import java.net.InetSocketAddress

import org.apache.avro.io._
import org.apache.avro.ipc._
import org.apache.avro.specific._

import scala.collection.jcl.Conversions._

case class RemoteNode(hostname:String, port: Int) {
    private val socketAddress = new InetSocketAddress(hostname, port) 
    def getInetSocketAddress:InetSocketAddress = socketAddress 
}

trait AvroChannelManager[SendMsgType <: SpecificRecord, RecvMsgType <: SpecificRecord] {
    def startBulk(chan: RemoteNode)
    def endBulk(chan: RemoteNode)
	def sendMessage(dest: RemoteNode, msg: SendMsgType):Unit 
	def receiveMessage(src: RemoteNode, msg: RecvMsgType):Unit
}

abstract class AvroChannelManagerImpl[SendMsgType <: SpecificRecord,RecvMsgType <: SpecificRecord](implicit sendManifest: scala.reflect.Manifest[SendMsgType], recvManifest: scala.reflect.Manifest[RecvMsgType])
    extends AvroChannelManager[SendMsgType, RecvMsgType]{
	private val selector = Selector.open()
	private var continueSelecting = true
	private val channels = new java.util.concurrent.ConcurrentHashMap[RemoteNode, ChannelState]
  private val msgReader = new SpecificDatumReader(recvManifest.erasure.asInstanceOf[Class[RecvMsgType]])
	private val msgWriter = new SpecificDatumWriter(sendManifest.erasure.asInstanceOf[Class[SendMsgType]])
	private val msgRecvClass = recvManifest.erasure.asInstanceOf[Class[RecvMsgType]]

	private class ChannelState(val chan: SocketChannel, var recvMessage: Boolean, var buff: ByteBuffer)

	private val selectionThread = new Thread("Channel Manager Selector") {
    override def run() = {
      while(continueSelecting) {
        val numKeys = selector.select(1000)
        val keys = selector.selectedKeys()

        keys.foreach(k => {
          val chan = k.channel.asInstanceOf[SocketChannel]
          val state = k.attachment.asInstanceOf[ChannelState]
					val addr = chan.socket.getInetAddress.getHostName
					val port = chan.socket.getPort

					if(state.recvMessage) {
						chan.read(state.buff)
						if(state.buff.remaining == 0) {
							val addr = chan.socket.getInetAddress.getHostName
							val port = chan.socket.getPort
							state.buff.rewind
							val inStream = new BinaryDecoder(new ByteBufferInputStream(java.util.Arrays.asList(state.buff)))
							val msg = msgRecvClass.newInstance()
							msgReader.read(msg.asInstanceOf[Object], inStream).asInstanceOf[RecvMsgType]
							state.recvMessage = false
							state.buff = ByteBuffer.allocate(4)
							receiveMessage(RemoteNode(addr, port), msg)
						}
					}
					else {
						chan.read(state.buff)
						if(state.buff.remaining == 0) {
							state.buff.rewind
							state.recvMessage = true
							val size = state.buff.getInt
							//println(size)
							state.buff = ByteBuffer.allocate(size)
						}
					}
        })
				keys.clear()
      }
    }
  }
	selectionThread.start

  private def watchChannel(chan: SocketChannel): ChannelState = {
		val state = new ChannelState(chan, false, ByteBuffer.allocate(4))
    chan.configureBlocking(false)
    chan.register(selector, SelectionKey.OP_READ, state)
		state
  }

	private def getOrOpenChannel(dest: RemoteNode): ChannelState = {
		if(!channels.containsKey(dest)) {
			val newChannel = SocketChannel.open(new InetSocketAddress(dest.hostname, dest.port))
			val state = watchChannel(newChannel)

			if(channels.putIfAbsent(dest, state) != null) {
				newChannel.close()
			}
		}
		channels.get(dest)
	}

	def startListener(port: Int): Unit = {
		val serverSock = ServerSocketChannel.open()
		serverSock.socket().bind(new InetSocketAddress(port))
		val thread = new Thread("Server Socket Port " + port) {
			override def run() = {
				while(true) {
					val newChan = serverSock.accept()
					newChan.configureBlocking(false)
					val addr = newChan.socket.getInetAddress.getHostName
					val port = newChan.socket.getPort
					channels.put(RemoteNode(addr, port), watchChannel(newChan))
				}
			}
		}
		thread.start
	}

	override def sendMessage(dest: RemoteNode, msg: SendMsgType): Unit = {
		val state = getOrOpenChannel(dest)
		val stream = new java.io.ByteArrayOutputStream(8192)
		val enc = new BinaryEncoder(stream)
		msgWriter.write(msg.asInstanceOf[Object], enc)

		val msgLength = ByteBuffer.allocate(4).putInt(stream.size)
		msgLength.rewind
		state.chan.write(Array(msgLength, ByteBuffer.wrap(stream.toByteArray)))
	}

	def closeConnections(): Unit = {
		channels.clear()
	}

    override def startBulk(chan: RemoteNode) = {}
    override def endBulk(chan: RemoteNode)   = {}

	//def receiveMessage(src: RemoteNode, msg: RecvMsgType): Unit
}

//class SampleChannelManager extends AvroChannelManager[Record, Record] {
//	def receiveMessage(src: RemoteNode, msg: Record): Unit = {
//		println(src + " " + msg.key + " " + msg.value)
//	}
//}
