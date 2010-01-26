package edu.berkeley.cs.scads.comm

import java.nio._
import java.nio.channels._
import java.net.InetSocketAddress

import org.apache.avro.io._
import org.apache.avro.ipc._
import org.apache.avro.specific._

import scala.collection.jcl.Conversions._

case class RemoteNode(hostname:String, port: Int)

abstract class AvroChannelManager[SendMsgType <: SpecificRecord, RecvMsgType <: SpecificRecord](implicit sendManifest: scala.reflect.Manifest[SendMsgType], recvManifest: scala.reflect.Manifest[RecvMsgType]){
	val selector = Selector.open()
	var continueSelecting = true
	val channels = new java.util.concurrent.ConcurrentHashMap[RemoteNode, ChannelState]
  val msgReader = new SpecificDatumReader(sendManifest.erasure)
	val msgWriter = new SpecificDatumWriter(recvManifest.erasure)

	class ChannelState(val chan: SocketChannel, var recvMessage: Boolean, var buff: ByteBuffer)

	val selectionThread = new Thread("Channel Manager Selector") {
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
							val msg = msgReader.read(null, inStream).asInstanceOf[RecvMsgType]
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
							println(size) 
							state.buff = ByteBuffer.allocate(size)
						}
					}
        })
				keys.clear()
      }
    }
  }
	selectionThread.start

  protected def watchChannel(chan: SocketChannel): ChannelState = {
		val state = new ChannelState(chan, false, ByteBuffer.allocate(4)) 
    chan.configureBlocking(false)
    chan.register(selector, SelectionKey.OP_READ, state)
		state
  }

	protected def getOrOpenChannel(dest: RemoteNode): ChannelState = {
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

	def sendMessage(dest: RemoteNode, msg: SendMsgType): Unit = {
		val state = getOrOpenChannel(dest)
		val stream = new java.io.ByteArrayOutputStream(8192)
		val enc = new BinaryEncoder(stream)
		msgWriter.write(msg, enc)
		state.chan.write(ByteBuffer.wrap(stream.toByteArray))
	}

	def closeConnections(): Unit = {
		channels.clear()	
	}

	def receiveMessage(src: RemoteNode, msg: RecvMsgType): Unit
}

class SampleChannelManager extends AvroChannelManager[Record, Record] {
	def receiveMessage(src: RemoteNode, msg: Record): Unit = {
		println(src + " " + msg.key + " " + msg.value)
	}
}
