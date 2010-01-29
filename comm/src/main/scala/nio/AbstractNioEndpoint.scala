package edu.berkeley.cs.scads.comm

import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.channels.spi.SelectorProvider
import java.util.{List => JList, LinkedList, HashMap}
//import java.util.concurrent.{BlockingQueue,LinkedBlockingQueue}
//import java.util.concurrent.ConcurrentHashMap

import java.util.concurrent.Executor

import org.apache.avro.specific.SpecificRecord

import scala.collection.mutable.ListBuffer

import org.apache.log4j.Logger

abstract class AbstractNioEndpoint(
        protected val hostAddress: InetSocketAddress, 
        protected val readExecutor: Executor,
        protected val channelHandler: ChannelHandler) {

    if (hostAddress == null || readExecutor == null || channelHandler == null)
        throw new IllegalArgumentException("Cannot pass in null values to constructor")

    private val logger = Logger.getLogger("AbstractNioEndpoint")

	protected val selector = SelectorProvider.provider.openSelector
	protected val readBuffer: ByteBuffer = ByteBuffer.allocate(8192)

    var bulkBufferSize = 64*1024

    case class QueueEntry(val contents: ByteBuffer, val callback: WriteCallback)
    
    //protected val dataQueue: BlockingQueue[QueueEntry] = new LinkedBlockingQueue[QueueEntry]

    protected val dataMapQueue = new HashMap[SocketChannel,JList[QueueEntry]]
    protected val bulkBufferQueue = new HashMap[SocketChannel, ByteBuffer]

    case class RegisterEntry(val socket: SocketChannel, val future: ConnectFuture)
    protected val registerQueue = new LinkedList[RegisterEntry]

    case class ChannelState(val buffer: ByteBuffer, var inMessage: Boolean, var messageSize: Int)  {
        def this(buffer: ByteBuffer) = this(buffer, false, 0)
    }

    class CircularByteBuffer(private var initialSize: Int) {
        private var bytes = Array[Byte](initialSize)
        private var head = 0
        private var size = 0

        def append(toAppend: Array[Byte]) = {
            if (bytesRemaining < toAppend.length) {
                // expand
                val newBytes = Array[ByteSize](bytes.length*2) // double size (exponential growth)
                if (head+size < bytes.length+1)
                    System.arraycopy(bytes, head, newBytes, 0, size) 
                else {
                    System.arraycopy(bytes, head, newBytes, 0, bytes.length-head)
                    System.arraycopy(bytes, 0, newBytes, bytes.length-head, size-(bytes.length-head))
                }
                head = 0
            }
            val tailStart = (head + size) % bytes.length
            if (tailStart > head) {
                // copy until end
                val toCopy = Math.min(toAppend.length, bytes.length-tailStart)
                System.arraycopy(toAppend, 0, bytes, tailStart, toCopy)
                
                // now copy into beginning
                if (toCopy < toAppend.length)
                    System.arraycopy(toAppend, toCopy, bytes, 0, toAppend.length-toCopy)
            } else {
                System.arraycopy(toAppend, 0, bytes, tailStart, bytes.length)
            }
            size += toAppend.length
        }

        def consumeBytes(length: Int):Array[Byte] = {
            if (length > size) 
                throw new IllegalArgumentException("Cannot consume more bytes than there are")
            val rtn = new Array[Byte](length)
            if (head + size < bytes.length + 1)
                System.arraycopy(bytes, head, rtn, 0, length)
            else {
                System.arraycopy(bytes, head, rtn, 0, bytes.length-head)
                System.arraycopy(bytes, 0, rtn, bytes.length-head, length-(bytes.length-head))
            }
            head = (head + length) % bytes.length
            size -= length
            rtn
        }

        def consumeInt:Int = {
            val bytea = consumeBytes(4)
            // little endian
            (bytea(3).toInt & 0xFF) << 24 | (bytea(2).toInt & 0xFF) << 16 | (bytea(1).toInt & 0xFF) << 8 | (bytea(0).toInt & 0xFF)
        }

        private def bytesRemaining:Int = bytes.length - size

        def size:Int = size
        

    }

    protected val channelQueue = new HashMap[SocketChannel, ChannelState]

    protected def fireWriteAndSelectLoop = {
        new Thread(new Runnable {
            override def run = writeLoop
        }).start
        new Thread(new Runnable {
            override def run = selectLoop
        }).start
    }

    protected def writeLoop = {
        while(true) {
            try {
                val callbacks = new ListBuffer[WriteCallback]
                dataMapQueue.synchronized {
                    val channelSet = dataMapQueue.keySet
                    if (channelSet.isEmpty)
                        dataMapQueue.wait
                    val iterator = channelSet.iterator
                    while (iterator.hasNext) {
                        val channel = iterator.next
                        if (!channel.isConnected)
                            dataMapQueue.remove(channel)
                        val queue = dataMapQueue.get(channel)
                        var keepTrying = !queue.isEmpty
                        while (keepTrying) {
                            val buffer = queue.get(0).contents
                            channel.write(buffer)
                            if (buffer.remaining > 0) {
                                logger.debug("channel: " + channel)
                                logger.debug("buffer could not be written in its entirely")
                                keepTrying = false
                            } else {
                                //logger.debug("for channel: " + channel)
                                //logger.debug("successfully wrote buffer: " + buffer)
                                callbacks.append(queue.get(0).callback)
                                queue.remove(0)
                                keepTrying = !queue.isEmpty
                            }
                        }
                        if (queue.isEmpty)
                            dataMapQueue.remove(channel)
                    }
                }
                callbacks.foreach(_.writeFinished)
            } catch {
                case e: Exception => channelHandler.handleException(e)
            }
        }
    }
        
    protected def selectLoop = {
        while(true) {
            try {
                registerQueue.synchronized {
                    val iter = registerQueue.iterator
                    while (iter.hasNext) {
                        val entry = iter.next
                        entry.socket.register(selector, SelectionKey.OP_CONNECT, entry.future)
                        //logger.debug("registered socket: " + entry.socket)
                    }
                    registerQueue.clear
                }
                selector.select
                //logger.debug("selectLoop awaken")
                val selectedKeys = selector.selectedKeys.iterator
                while (selectedKeys.hasNext) {
                    val key = selectedKeys.next
                    selectedKeys.remove
                    if (key.isValid) 
                        if (key.isAcceptable) 
                            accept(key)
                        else if (key.isConnectable)
                            connect(key)
                        else if (key.isReadable)
                            read(key)
                }
            } catch {
                case e: Exception => channelHandler.handleException(e)
            }
        }
    }

    /**
     * Blocks until all the data is sent out the channel
     */
    def sendImmediately(socket: SocketChannel, data: Array[Byte]) = {
        val buffer = ByteBuffer.wrap(data)
        while (buffer.remaining > 0) {
            socket.write(buffer) 
        }
    }

    private def encodeByteBuffer(data: ByteBuffer): ByteBuffer = {
        val newBuffer = ByteBuffer.allocate(4 + data.remaining)
        newBuffer.putInt(data.remaining) 
        newBuffer.put(data)
        newBuffer
    }

    /**
     * Asynchrounously send by just adding to send queue and returning
     */
    def send(socket: SocketChannel, data: ByteBuffer, callback: WriteCallback, encode: Boolean):Unit = {
        //logger.debug("send() called with data: " + data)
        dataMapQueue.synchronized {
            var queue = dataMapQueue.get(socket)
            if (queue == null) {
                queue = new LinkedList[QueueEntry]
                dataMapQueue.put(socket, queue)
            }
            val towrite = encode match {
                case false => data,
                case true  => encodeByteBuffer(data)
            }
            queue.add(new QueueEntry(toWrite, callback))
            dataMapQueue.notifyAll
        }
    }

    def send(socket: SocketChannel, data: ByteBuffer, encode: Boolean):Unit = send(socket,data,null,encode)

    def send(socket: SocketChannel, data: Array[Byte], callback: WriteCallback, encode: Boolean):Unit = 
        send(socket, ByteBuffer.wrap(data), callback, encode)

    def send(socket: SocketChannel, data: Array[Byte], encode: Boolean):Unit = send(socket,data,null,encode)

    private def sizeOfMessage(data: ByteBuffer; encode: Boolean): Int = encode match {
        case false => data.remaining,
        case true  => data.remaining + 4 
    }

    /**
     * Asynchrounously send the data as part of a bulk loading. This means
     * that there is no guarantee when the data will be placed in the
     * send queues. This is meant to be used in a loop where you are trying
     * to send many byte arrays at once. Also there is no callback support
     * here because bulk sends are lumped together into one byte buffer,
     * so the semantics of callbacks don't really make sense
     */
    def sendBulk(socket: SocketChannel, data: ByteBuffer, encode: Boolean):Unit = {
        //TODO: be more robust and actually slice up the data appropriate
        // instead of just mandating it fit into one buffer. However as a
        // general rule of thumb this isn't that important, because there's
        // really no need to buffer like this unless size of data buffer is
        // much smaller than size of internal buffer
        if (sizeOfMessage(data, encode) > bulkBufferSize)
            throw new IllegalArgumentException("Need to increase the bulk buffer size")
        bulkBufferQueue.synchronized {
            var buffer = bulkBufferQueue.get(socket)
            if (buffer == null) {
                buffer = ByteBuffer.allocate(bulkBufferSize)
                bulkBufferQueue.put(socket, buffer)
            }

            if (sizeOfMessage(data,encode) < buffer.remaining) {
                // buffer is full, flush it and clear
                buffer.rewind
                send(socket, buffer, false) // no encoding since we already did
                buffer = ByteBuffer.allocate(bulkBufferSize)
            }

            buffer.put(encodeByteBuffer(data))

            if (buffer.remaining == 0) {
                buffer.rewind
                send(socket, buffer, false)
                bulkBufferQueue.remove(socket)
            }
        }
    }

    def sendBulk(socket: SocketChannel, data: Array[Byte]):Unit = 
        sendBulk(socket, ByteBuffer.wrap(data))


    /**
     * Flush any internal buffering and put whatever is left in the buffer
     * into the send queues. This should be called only after you make a
     * sequeence of calls to sendBulk
     */
    def flushBulk(socket: SocketChannel) = {
        bulkBufferQueue.synchronized {
            val buffer = bulkBufferQueue.get(socket)
            if (buffer != null) {
                buffer.rewind
                send(socket, buffer, false)
                bulkBufferQueue.remove(socket)
            }
        }
    }

    protected def accept(key: SelectionKey) = {}

    protected def connect(key: SelectionKey) = {} 

    protected def read(key: SelectionKey):Unit = {
        //logger.debug("read() called on channel: " + key.channel)
        val socketChannel = key.channel.asInstanceOf[SocketChannel]
        var channelState = channelQueue.get(socketChannel)
        if (channelState == null) {
            channelState = new ChannelState(ByteBuffer.allocate(1024))
            channelQueue.put(channelState)
        }

        readBuffer.clear
        var numRead = 0
		try {
			numRead = socketChannel.read(readBuffer)
		} catch {
            case e: IOException => 
                // The remote forcibly closed the connection, cancel
                // the selection key and close the channel.
                channelQueue.remove(socketChannel)
                key.cancel
                socketChannel.close
                return
		}
		if (numRead == -1) {
			// Remote entity shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
            channelQueue.remove(socketChannel)
			key.channel.close
			key.cancel
			return
		}

        var bytesProcessed = 0
        while (bytesProcessed < numRead) {
            if (!channelState.inMessage) {
                if (channelState.buffer.position >= 4) {
                    channelState.buffer.mark(4)
                    channelState.inMessage = true
                    channelState.messageSize = channelState.buffer.getInt  
                } else {
                    val toRead = Math.min(numRead-bytesProcessed, Math.min(4, 4-channelState.buffer.remaining))


                }
            } else {


            }
        }

        // now complete the read in a separate thread
        readExecutor.execute(new Runnable {
            override def run = {
                channelHandler.processData(socketChannel, readBuffer.array, numRead)
            }
        })
    }

}
