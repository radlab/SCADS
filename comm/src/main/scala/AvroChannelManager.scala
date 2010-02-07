package edu.berkeley.cs.scads.comm

import java.nio._
import java.nio.channels._
import java.net.InetSocketAddress

import org.apache.avro.io._
import org.apache.avro.ipc._
import org.apache.avro.specific._

import org.apache.log4j.Logger

import scala.collection.jcl.Conversions._

case class RemoteNode(hostname:String, port: Int)

abstract class AvroChannelManager[SendMsgType <: SpecificRecord, RecvMsgType <: SpecificRecord](implicit sendManifest: scala.reflect.Manifest[SendMsgType], recvManifest: scala.reflect.Manifest[RecvMsgType]){
	private val selector = Selector.open()
	private var continueSelecting = true
	private val channels = new java.util.concurrent.ConcurrentHashMap[RemoteNode, ChannelState]
  private val msgReader = new SpecificDatumReader(recvManifest.erasure.asInstanceOf[Class[RecvMsgType]])
	private val msgWriter = new SpecificDatumWriter(sendManifest.erasure.asInstanceOf[Class[SendMsgType]])
	private val msgRecvClass = recvManifest.erasure.asInstanceOf[Class[RecvMsgType]]

    private val logger = Logger.getLogger("AvroChannelManager")

    private val registerQueue = new java.util.LinkedList[ChannelState]

	private class ChannelState(val chan: SocketChannel, var recvMessage: Boolean, var buff: ByteBuffer)

	private val selectionThread = new Thread("Channel Manager Selector") {
    override def run() = {
      while(continueSelecting) {
        registerQueue.synchronized {
          val iter = registerQueue.iterator
          while (iter.hasNext) {
            val chanstate = iter.next
            logger.debug("registering channel: " + chanstate.chan)
            chanstate.chan.register(selector, SelectionKey.OP_READ, chanstate)
            logger.debug("done registering")
          }
          registerQueue.clear
          registerQueue.notifyAll
        }

        logger.debug("waiting for selector keys")
        val numKeys = selector.select(1000)

        val keys = selector.selectedKeys()
        keys.foreach(k => {

          val chan = k.channel.asInstanceOf[SocketChannel]
          val state = k.attachment.asInstanceOf[ChannelState]
					val addr = chan.socket.getInetAddress.getHostName
					val port = chan.socket.getPort

            logger.debug("got selector keys from: " + addr + ", " + port)

					if(state.recvMessage) {
                        logger.debug("recvMessage is true, reading from buffer")
						chan.read(state.buff)
						if(state.buff.remaining == 0) {
                            logger.debug("buffer is full, creating message")
							val addr = chan.socket.getInetAddress.getHostName
							val port = chan.socket.getPort
							state.buff.rewind
							val inStream = new BinaryDecoder(new ByteBufferInputStream(java.util.Arrays.asList(state.buff)))
							val msg = msgRecvClass.newInstance()
							msgReader.read(msg, inStream).asInstanceOf[RecvMsgType]
							state.recvMessage = false
							state.buff = ByteBuffer.allocate(4)
							receiveMessage(RemoteNode(addr, port), msg)
						}
					}
					else {
                        logger.debug("reading the header")
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

  private def watchChannel(chan: SocketChannel): ChannelState = {
		val state = new ChannelState(chan, false, ByteBuffer.allocate(4))
    chan.configureBlocking(false)
    registerQueue.synchronized {
      registerQueue.add(state)
      while (!chan.isRegistered)
        registerQueue.wait
    }
		state
  }

	private def getOrOpenChannel(dest: RemoteNode): ChannelState = {
        synchronized {
		if(!channels.containsKey(dest)) {
            logger.debug("getOrOpenChannel: new channel opened to: " + dest)
			val newChannel = SocketChannel.open(new InetSocketAddress(dest.hostname, dest.port))
            assert(newChannel.isConnected)
			val state = watchChannel(newChannel)

			if(channels.putIfAbsent(dest, state) != null) {
				newChannel.close()
			}
		}
		channels.get(dest)
        }
	}

	def startListener(port: Int): Unit = {
		val serverSock = ServerSocketChannel.open()
		serverSock.socket().bind(new InetSocketAddress(port))
		val thread = new Thread("Server Socket Port " + port) {
			override def run() = {
				while(true) {
					val newChan = serverSock.accept()
                    logger.debug("new connection: " + newChan)
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
        logger.debug("sendMessage called, dest " + dest + " msg " + msg)
		val state = getOrOpenChannel(dest)
		val stream = new java.io.ByteArrayOutputStream(8192)
		val enc = new BinaryEncoder(stream)
		msgWriter.write(msg, enc)

		val msgLength = ByteBuffer.allocate(4).putInt(stream.size)
		msgLength.rewind
        logger.debug("sendMessage: msg being sent to: " + dest)
		state.chan.write(Array(msgLength, ByteBuffer.wrap(stream.toByteArray)))
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
