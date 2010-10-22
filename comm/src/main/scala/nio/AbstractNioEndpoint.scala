package edu.berkeley.cs.scads.comm

import java.io.{ByteArrayOutputStream,IOException}
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SelectableChannel
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.channels.spi.SelectorProvider
import java.util.{List => JList, LinkedList, HashMap}
import java.util.concurrent.ConcurrentHashMap

import java.util.concurrent.Executor

import org.apache.avro.specific.SpecificRecord

import scala.collection.mutable.ListBuffer

import net.lag.logging.Logger

import edu.berkeley.cs.scads.config._

class NioEndpoint(protected val channelHandler: ChannelHandler) {
  if (channelHandler == null)
    throw new IllegalArgumentException("Cannot pass in null values to constructor")

  private val logger = Logger()

  var acceptEventHandler: NioAcceptEventHandler = null
  var connectEventHandler: NioConnectEventHandler = null

  protected var serverChannel: ServerSocketChannel = null
  protected var initialized = false
  protected var isListening = false

  protected val selector = SelectorProvider.provider.openSelector
  protected val readBuffer: ByteBuffer = ByteBuffer.allocate(8192)

  protected var bulkBufferSize = 64*1024

  case class QueueEntry(val contents: ByteBuffer, val callback: WriteCallback)
  case class RegisterEntry(val socket: SelectableChannel, val future: ConnectFuture, val interestOp: Int)

  //protected val dataQueue: BlockingQueue[QueueEntry] = new LinkedBlockingQueue[QueueEntry]

  protected val dataMapQueue = new HashMap[SocketChannel,JList[QueueEntry]]
  protected val bulkBufferQueue = new HashMap[SocketChannel, ByteArrayOutputStream]
  protected val registerQueue = new LinkedList[RegisterEntry]
  protected val connectionMap = new ConcurrentHashMap[InetSocketAddress,SocketChannel]
  protected val channelQueue = new HashMap[SocketChannel, ChannelState]

  protected def registerInetSocketAddress(addr: InetSocketAddress, chan: SocketChannel) = connectionMap.put(addr, chan)
  def getChannelForInetSocketAddress(addr: InetSocketAddress):SocketChannel = connectionMap.get(addr)


  private lazy val useTcpNoDelay = Config.config.getBool("scads.comm.tcpNoDelay", true)

  case class ChannelState(val buffer: CircularByteBuffer, var inMessage: Boolean, var messageSize: Int)  {
    def this(buffer: CircularByteBuffer) = this(buffer, false, 0)
    def reset = {
      inMessage = false
      messageSize = 0
    }
  }

  case class InvalidMessageSizeException(val size: Int) extends RuntimeException("Invalid size, must be >= 0: " + size)

  // TODO: replace with BlockingFuture
  class ConnectFuture(val clientSocket: SocketChannel, private var alreadyDone: Boolean) {
    def await = {
      synchronized {
        if (!alreadyDone) {
          wait
        }
      }
    }

    def finished = {
      synchronized {
        alreadyDone = true
        notifyAll
      }
    }
  }

  protected def fireWriteAndSelectLoop = {
    new Thread(new Runnable {
      override def run = writeLoop
    }).start
    new Thread(new Runnable {
      override def run = selectLoop
    }).start
  }

  private def mergeQueue(queue: JList[QueueEntry]) = {
    var size = 0
    var iter = queue.iterator
    while (iter.hasNext) {
      size += iter.next.contents.remaining
    }
    val newBuf = ByteBuffer.allocate(size)
    val callbacks = new LinkedList[WriteCallback]
    iter = queue.iterator
    while (iter.hasNext) {
      val entry = iter.next
      newBuf.put(entry.contents)
      if (entry.callback != null)
        callbacks.add(entry.callback)
    }
    newBuf.rewind
    queue.clear
    val newWriteCallback = callbacks.size match {
      case 0 => null
      case _ => new WriteCallback {
        override def writeFinished = {
          val callbackIter = callbacks.iterator
          while (callbackIter.hasNext) callbackIter.next.writeFinished
        }
      }
    }
    queue.add(new QueueEntry(newBuf, newWriteCallback))
  }

  protected def writeLoop = {
    val callbacks = new LinkedList[WriteCallback]
    while(true) {
      try {
        dataMapQueue.synchronized {
          val channelSet = dataMapQueue.keySet
          if (channelSet.isEmpty)
            dataMapQueue.wait
          val iterator = channelSet.iterator
          while (iterator.hasNext) {
            val channel = iterator.next
            if (!channel.isConnected) {
              iterator.remove
            } else {
              val queue = dataMapQueue.get(channel)
              if (queue.size > 1)
                mergeQueue(queue)
              var attempts = 5
              while (attempts > 0) {
                var keepTrying = !queue.isEmpty
                while (keepTrying) {
                  val buffer = queue.get(0).contents
                  channel.write(buffer)
                  if (buffer.remaining > 0) {
                    //logger.debug("channel: " + channel)
                    //logger.debug("buffer could not be written in its entirely")
                    keepTrying = false
                  } else {
                    //logger.debug("for channel: " + channel)
                    //logger.debug("successfully wrote buffer: " + buffer)
                    if (queue.get(0).callback != null)
                      callbacks.add(queue.get(0).callback)
                    queue.remove(0)
                    keepTrying = !queue.isEmpty
                  }
                }
                attempts -= 1
              }
              if (queue.isEmpty) iterator.remove
            }
          }
        }
        val iter = callbacks.iterator
        while (iter.hasNext)
        iter.next.writeFinished
        callbacks.clear
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
            if (entry.interestOp == SelectionKey.OP_CONNECT)
              entry.socket.register(selector, SelectionKey.OP_CONNECT, entry.future)
            else if (entry.interestOp == SelectionKey.OP_ACCEPT)
              entry.socket.register(selector, SelectionKey.OP_ACCEPT)
            else
              throw new IllegalStateException("should not have this interest op")
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
  def sendImmediately(socket: SocketChannel, data: ByteBuffer, encode: Boolean) = {
    val buffer = encode match {
      case false => data
      case true  => encodeByteBuffer(data)
    }
    while (buffer.remaining > 0) {
      socket.write(buffer)
    }
  }

  private def encodeByteBuffer(data: ByteBuffer): ByteBuffer = {
    //val newBuffer = ByteBuffer.allocate(4 + data.remaining)
    //newBuffer.putInt(data.remaining)
    //newBuffer.put(data)
    //newBuffer.rewind
    //newBuffer

    // this seems to run a little faster
    val buf = new Array[Byte](4 + data.remaining)
    buf(0) = ((data.remaining >> 24) & 0xFF).toByte
    buf(1) = ((data.remaining >> 16) & 0xFF).toByte
    buf(2) = ((data.remaining >> 8) & 0xFF).toByte
    buf(3) = (data.remaining & 0xFF).toByte
    data.get(buf, 4, data.remaining)
    ByteBuffer.wrap(buf)
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
      val toWrite = if (!encode) {
        data
      } else {
        encodeByteBuffer(data)
      }
      queue.add(new QueueEntry(toWrite, callback))
      dataMapQueue.notifyAll
    }
  }

  def send(socket: SocketChannel, data: ByteBuffer, encode: Boolean):Unit = send(socket,data,null,encode)

  def send(socket: SocketChannel, data: Array[Byte], callback: WriteCallback, encode: Boolean):Unit =
    send(socket, ByteBuffer.wrap(data), callback, encode)

  def send(socket: SocketChannel, data: Array[Byte], encode: Boolean):Unit = send(socket,data,null,encode)

  private def sizeOfMessage(data: ByteBuffer, encode: Boolean): Int = encode match {
    case false => data.remaining
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
        //logger.debug("buffer was null, so allocating")
        buffer = new ByteArrayOutputStream(bulkBufferSize)
        bulkBufferQueue.put(socket, buffer)
      }

      //logger.debug("--")
      //logger.debug("sizeOfMessage: " + sizeOfMessage(data, encode))
      //logger.debug("remaining: " + buffer.remaining)

      if (sizeOfMessage(data,encode) > bulkBufferSize - buffer.size) {
        // buffer is full, flush it and clear
        logger.debug("flushing and realloc buffer")
        send(socket, ByteBuffer.wrap(buffer.toByteArray), false) // no encoding since we already did
        buffer.reset
      }

      val toPut = encode match {
        case false => data
        case true  => encodeByteBuffer(data)
      }

      //logger.debug("toPut size: " + toPut.remaining)
      //logger.debug("buffer remaining size: " + buffer.remaining)
      buffer.write(toPut.array)

      //logger.debug("after: " + buffer.remaining)

      if (buffer.size == bulkBufferSize) {
        logger.debug("flushing and removing buffer")
        send(socket, ByteBuffer.wrap(buffer.toByteArray), false)
        buffer.reset
      }
    }
  }

  def sendBulk(socket: SocketChannel, data: Array[Byte], encode: Boolean):Unit =
    sendBulk(socket, ByteBuffer.wrap(data),encode)


  /**
  * Flush any internal buffering and put whatever is left in the buffer
  * into the send queues. This should be called only after you make a
  * sequeence of calls to sendBulk
  */
  def flushBulk(socket: SocketChannel) = {
    bulkBufferQueue.synchronized {
      val buffer = bulkBufferQueue.get(socket)
      if (buffer != null) {
        send(socket, ByteBuffer.wrap(buffer.toByteArray), false)
        bulkBufferQueue.remove(socket)
      }
    }
  }

  def flushAllBulk = {
    logger.debug("flushAllBulk() called")
    bulkBufferQueue.synchronized {
      val iter = bulkBufferQueue.keySet.iterator
      while (iter.hasNext) {
        val socket = iter.next
        val buffer = bulkBufferQueue.get(socket)
        if (buffer != null)  {
          logger.debug("socket " + socket + " has non-empty buffer")
          send(socket, ByteBuffer.wrap(buffer.toByteArray), false)
        }
      }
      bulkBufferQueue.clear
    }
  }

  def getListeningAddr(): InetAddress = {
    synchronized {
      if (isListening)
        serverChannel.socket.getInetAddress
      else
        null
    }
  }

  def getListeningPort(): Int = {
    synchronized {
      if (isListening)
        serverChannel.socket.getLocalPort
      else
        0
    }
  }

  def serve(hostAddress: InetSocketAddress) = {
    synchronized {
      if (isListening) throw new IllegalStateException("Already listening")

      serverChannel = ServerSocketChannel.open
      serverChannel.configureBlocking(false)
      serverChannel.socket.bind(hostAddress)
      var shouldInitialize = false
      synchronized {
        if (!initialized) {
          shouldInitialize = true
          initialized = true
        }
      }
      if (shouldInitialize) {
        serverChannel.register(selector, SelectionKey.OP_ACCEPT)
        fireWriteAndSelectLoop
      } else {
        registerQueue.synchronized {
          registerQueue.add(new RegisterEntry(serverChannel, null, SelectionKey.OP_ACCEPT))
        }
        selector.wakeup
      }
      isListening = true
    }
    logger.info("Now serving on: " + hostAddress)
  }

  protected def accept(key: SelectionKey):Unit = {
    logger.debug("accept() called with: " + key)
    val serverSocketChannel = key.channel.asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept
    socketChannel.configureBlocking(false)
    socketChannel.socket.setTcpNoDelay(useTcpNoDelay)
    socketChannel.register(selector, SelectionKey.OP_READ)
    registerInetSocketAddress(new InetSocketAddress(socketChannel.socket.getInetAddress, socketChannel.socket.getPort), socketChannel)
    if (acceptEventHandler != null) acceptEventHandler.acceptEvent(socketChannel)
  }

  protected def connect(key: SelectionKey):Unit = {
    logger.debug("connect() called with: %s", key)
    val socketChannel = key.channel.asInstanceOf[SocketChannel]
    try {
      socketChannel.finishConnect
    } catch {
      case e: IOException =>
        // Cancel the channel's registration with our selector
        logger.error("failed to connect to %s with error: %s", key.channel, e.getMessage)
      key.cancel
      return
    }
    logger.debug("trying to register OP_READ interest")
    key.interestOps(SelectionKey.OP_READ)
    logger.debug("registing INET socket address now")
    registerInetSocketAddress(
      new InetSocketAddress(socketChannel.socket.getInetAddress,socketChannel.socket.getPort),
      socketChannel)
    logger.debug("calling event handler")
    if (connectEventHandler != null) connectEventHandler.connectEvent(socketChannel)
    logger.debug("calling finished() on future")
    key.attachment.asInstanceOf[ConnectFuture].finished
    logger.debug("connect is now finished")
  }

  /**
  * Connect is thread safe
  */
  def connect(hostAddress: InetSocketAddress):ConnectFuture = {
    logger.info("Connecting to %s", hostAddress)
    synchronized {
      if (!initialized) {
        fireWriteAndSelectLoop
        initialized = true
      }
    }
    val clientSocket = SocketChannel.open
    clientSocket.socket.setTcpNoDelay(useTcpNoDelay)
    clientSocket.configureBlocking(false)
    val connected = clientSocket.connect(hostAddress)
    val future = new ConnectFuture(clientSocket, connected)
    if (connected) {
      logger.info("Connected!")
    } else {
      logger.info("Not connected, deferring connection")
      //clientSocket.register(selector, SelectionKey.OP_CONNECT, future)
      registerQueue.synchronized {
        registerQueue.add(new RegisterEntry(clientSocket, future, SelectionKey.OP_CONNECT))
      }
    }
    logger.debug("waking up selector")
    selector.wakeup
    future
  }

  protected def read(key: SelectionKey):Unit = {
    //logger.debug("read() called on channel: " + key.channel)
    val socketChannel = key.channel.asInstanceOf[SocketChannel]
    var channelState = channelQueue.get(socketChannel)
    if (channelState == null) {
      channelState = new ChannelState(new CircularByteBuffer)
      channelQueue.put(socketChannel, channelState)
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

    val readByteArray = new Array[Byte](numRead)
    readBuffer.rewind
    readBuffer.get(readByteArray)

    //logger.debug("read(): ")
    //(0 until readByteArray.length).foreach( i => {
    //    logger.debug("    pos("+i+"):" + (0xFF & readByteArray(i).toInt))
    //})
    //logger.debug("--")

    // read data into queue
    channelState.buffer.append(readByteArray)

    // process as much of the buffer as possible given the data so far
    var shouldContinue = !channelState.buffer.isEmpty
    while (shouldContinue) {
      if (!channelState.inMessage && channelState.buffer.size >= 4) {
        val messageSize = channelState.buffer.consumeInt
        if (messageSize < 0)
          throw new InvalidMessageSizeException(messageSize)
        channelState.inMessage = true
        channelState.messageSize = messageSize
        //logger.debug("read(): found messageSize: " + messageSize)
      } else if (channelState.inMessage && channelState.buffer.size >= channelState.messageSize) {
        val message = channelState.buffer.consumeBytes(channelState.messageSize)
        val size = channelState.messageSize
        channelHandler.processData(socketChannel, message, size)
        channelState.reset
      } else {
        shouldContinue = false
      }
    }
  }
}
