package edu.berkeley.cs.scads.comm
package oio

import java.io._
import java.net._
import java.util.concurrent._
import atomic._

import org.apache.avro._
import io._
import specific._

import net.lag.logging.Logger

import scala.reflect.Manifest.classType
import edu.berkeley.cs.scads.config._

/**
 * Easier to instantiate via reflection
 */
class DefaultBlockingChannelManager[S <: SpecificRecord, R <: SpecificRecord](
    recvMsg: (AvroChannelManager[S, R], RemoteNode, R) => Unit, sendClz: Class[S], recvClz: Class[R]) 
  extends BlockingChannelManager[S, R]()(classType(sendClz), classType(recvClz)) {
  override def receiveMessage(remoteNode: RemoteNode, msg: R) {
    recvMsg(this, remoteNode, msg)
  }
}

abstract class BlockingChannelManager[S <: SpecificRecord, R <: SpecificRecord](
    implicit sendManifest: Manifest[S], recvManifest: Manifest[R])
  extends AvroChannelManager[S, R] {

  protected val log = Logger()

  private lazy val useTcpNoDelay = Config.config.getBool("scads.comm.tcpNoDelay", true)

  // Avro serialization

  private val msgRecvClass = recvManifest.erasure.asInstanceOf[Class[R]]
  private val msgSendClass = sendManifest.erasure.asInstanceOf[Class[S]]

  private val msgReader = 
    new SpecificDatumReader[R](msgRecvClass.newInstance.getSchema)
  private val msgWriter = 
    new SpecificDatumWriter[S](msgSendClass.newInstance.getSchema)

  private val decoderFactory = new DecoderFactory
  decoderFactory.configureDirectDecoder(true)

  // Thread pools

  /** Thread pool for ServerSockets */
  private val listenerPool = 
    Executors.newCachedThreadPool

  /** Thread pool for Sockets */
  private val connectionPool =
    Executors.newCachedThreadPool // TODO: configure

  /** Thread pool for processing send queues */
  private val sendQueuePool =
    Executors.newCachedThreadPool // TODO: configure

  // Connection/Listener management

  /** Used to manage open connections */
  private val nodeToConnections = 
    new ConcurrentHashMap[InetSocketAddress, Connection]

  /** Used to manage open ports */
  private val portToListeners =
    new ConcurrentHashMap[Int, Listener]

  override def sendMessage(dest: RemoteNode, msg: S) {
    doSendMessage(dest, msg, false)
  }

  override def sendMessageBulk(dest: RemoteNode, msg: S) {
    doSendMessage(dest, msg, true)
  }

  private def doSendMessage(dest: RemoteNode, msg: S, flush: Boolean) {
    val os      = new PreallocByteArrayOutputStream(4, 512)
    val encoder = new BinaryEncoder(os)
    msgWriter.write(msg, encoder)

    val conn = getConnectionFor(dest)
    conn.send(os.underlyingBuffer, os.numPreAllocBytes, os.effectiveSize, flush)
  }

  private def getConnectionFor(dest: RemoteNode) = {
    val addr  = dest.getInetSocketAddress
    val conn0 = nodeToConnections.get(addr)
    if (conn0 ne null) conn0
    else {
      nodeToConnections.synchronized {
        val test0 = nodeToConnections.get(addr)
        if (test0 ne null) test0 // check again
        else {
          val newConn = connect(addr)
          val ret = nodeToConnections.put(addr, newConn)
          assert(ret eq null, "should not have overwritten a new connection")
          newConn
        }
      }
    }
  }

  override def flush { /* No-op */ }

  override def startListener(port: Int) {
    if (port < 0 || port > 65535)
      throw new IllegalArgumentException("Invalid port: %d".format(port))

    val list0 = portToListeners.get(port)
    if (list0 ne null) list0
    else {
      portToListeners.synchronized {
        val test0 = portToListeners.get(port)
        if (test0 ne null) test0 // check again
        else {
          val newListener = listen(port)
          val ret = portToListeners.put(port, newListener)
          assert(ret eq null, "should not have overwritten a new listener")
          newListener
        }
      }
    }
  }

  private def listen(port: Int) = {
    log.info("starting listener on port %d".format(port))
    // open the socket in the current thread
    val serverSocket = new ServerSocket(port)
    new Listener(serverSocket)
  }

  private def connect(addr: InetSocketAddress) = {
    require(addr ne null)

    log.info("opening new connection to address: %s".format(addr))
    // open the connection in the current thread
    val socket = new Socket
    socket.setTcpNoDelay(useTcpNoDelay)  // disable Nagle's algorithm
    socket.connect(addr, 60000) // wait up til 1 min for connect

    new Connection(socket, true) // start send/read threads immediately
  }

  class SendTask(val bytes: Array[Byte], val offset: Int, val length: Int, val flush: Boolean) {
    val future = new BlockingFuture[Unit]
  }

  trait RunnableHelper {
    def makeRunnable(f: => Unit) = new Runnable {
      override def run() { f }
    }
  }

  trait Stoppable {
    protected val continue = new AtomicBoolean(true)
    protected def doAfterStop()  {}
    def stop() { 
      if (continue.compareAndSet(true, false)) {
        doAfterStop()
      }
    }
    def isRunning = continue.get
    def isStopped = !continue.get
    protected def ensureRunning() {
      if (isStopped)
        throw new RuntimeException(this + ": Not running")
    }
  }

  /** Takes a valid socket. 
   * Spawns 2 threads- a listening thread, and a sending thread */
  class Connection(socket: Socket, startThreads: Boolean) 
    extends Stoppable with RunnableHelper {
    require(socket ne null)

    private val remoteAddress = 
      socket.remoteInetSocketAddress

    private val remoteNode = 
      remoteAddress.toRemoteNode 

    private lazy val localAddress =
      socket.localInetSocketAddress

    private val sendQueue = new LinkedBlockingQueue[SendTask]

    private val outputStream = socket.getOutputStream
    private val inputStream  = socket.getInputStream

    override def doAfterStop() {
      nodeToConnections.remove(remoteAddress)
      if (socket.isConnected) {
        // find the last task in queue, and wait on its future for up to 5
        // seconds
        val view = sendQueue.toArray(new Array[SendTask](sendQueue.size))
        if (view.length > 0) {
          val last = view(view.length - 1)
          try {
            last.future.await(5000)
          } catch {
            case _: FutureTimeoutException =>
              log.error("Could not drain send queue: took more than 5 seconds")
            case ex: FutureException =>
              log.error("Could not drain send queue: caught exception", ex)
          }
        }
      }
      socket.close()
    }

    if (startThreads)
      initialize()

    def initialize() {
      threadStarter
    }

    private object threadStarter {

      // start sending thread
      sendQueuePool.execute(makeRunnable {
        while (continue.get) {
          val task = sendQueue.take()
          try {
            outputStream.write(task.bytes, task.offset, task.length)
            if (task.flush)
              outputStream.flush()
            task.future.finish()
          } catch {
            case e: IOException =>
              log.error("Could not write send task", e)
              task.future.finishWithError(e)
              stop()
          }
        }
      })

      // start reading thread
      connectionPool.execute(makeRunnable {
        val dataInputStream = new DataInputStream(inputStream)
        while (continue.get) {
          try {
            val len = dataInputStream.readInt()
            if (len < 0) {
              log.error("Read bad input length: %d".format(len))
              stop()
            } else {
              val bytes = new Array[Byte](len)
              dataInputStream.readFully(bytes)
              val inStream = decoderFactory.createBinaryDecoder(bytes, null) 
              val msg = msgRecvClass.newInstance
              msgReader.read(msg, inStream)
              receiveMessage(remoteNode, msg)
            }
          } catch {
            case e: IOException =>
              log.error("Caught error in reading", e)
              stop()
          }
        }
      })
    
    }

    /** Does the message framing (using 4 bytes) */
    def send(bytes: Array[Byte], offset: Int, length: Int, flush: Boolean) {
      require(bytes ne null, "Bytes cannot be null")
      require(offset >= 0, "Offset cannot be negative")
      require(length >= 0, "Length cannot be negative")
      require((offset + length) <= bytes.length, "offset + length cannot exceed bounds of array: offset %d, length: %d".format(offset, length))

      ensureRunning()

      def writeInt(v: Int, b: Array[Byte], startPos: Int) {
        b(startPos)     = ((v >>> 24) & 0xff).toByte
        b(startPos + 1) = ((v >>> 16) & 0xff).toByte
        b(startPos + 2) = ((v >>> 8) & 0xff).toByte
        b(startPos + 3) = ((v & 0xff)).toByte
      }

      val task = 
        if (offset >= 4) {
          writeInt(length, bytes, offset - 4)
          new SendTask(bytes, offset - 4, length + 4, flush)
        } else {
          val newArray = new Array[Byte](length + 4)
          writeInt(length, newArray, 0) 
          System.arraycopy(bytes, offset, newArray, 4, length)
          new SendTask(newArray, 0, length + 4, flush)
        }

      sendQueue.offer(task)
    }

  }

  private final val V = new Object

  class Listener(serverSocket: ServerSocket) 
    extends Stoppable with RunnableHelper {
    require(serverSocket ne null)

    private val port = serverSocket.getLocalPort

    private val connectionsSet = 
      new ConcurrentHashMap[EphemeralConnection, Object]

    class EphemeralConnection(socket: Socket) extends Connection(socket, false) {
      override def doAfterStop() {
        super.doAfterStop()
        val test = connectionsSet.remove(this)
        if (test eq null) 
          log.error("ephemeral connection was not in connection set: %s".format(this))
      }
    }

    override def doAfterStop() {
      import scala.collection.JavaConversions._
      portToListeners.remove(port)
      connectionsSet.keySet.foreach(_.stop())
      connectionsSet.clear()
      serverSocket.close()
    }

    listenerPool.execute(makeRunnable {
      while (continue.get) {
        try {
          val client = serverSocket.accept()
          if (continue.get) {
            client.setTcpNoDelay(useTcpNoDelay) // disable Nagle's algorithm

            val conn = new EphemeralConnection(client)
            val prev = nodeToConnections.putIfAbsent(client.remoteInetSocketAddress, conn)
            if (prev ne null) {
              log.error("Unable to enter new remote connection into connection map: refusing connection")
              conn.stop()
            }
            connectionsSet.put(conn, V)
            conn.initialize() // start send/read threads
          }
        } catch {
          case e: IOException =>
            log.error("Caught error in accepting", e)
            stop()
        }
      }
    })

  }

  class RichSocket(socket: Socket) {
    def remoteInetSocketAddress = 
      new InetSocketAddress(socket.getInetAddress, socket.getPort)
    def localInetSocketAddress = 
      new InetSocketAddress(socket.getLocalAddress, socket.getLocalPort)
  }

  implicit def toRichSocket(socket: Socket): RichSocket = 
    new RichSocket(socket)

  class RichInetSocketAddress(addr: InetSocketAddress) {
    def toRemoteNode = 
      RemoteNode(addr.getHostName, addr.getPort)
  }

  implicit def toRichInetSocketAddress(addr: InetSocketAddress): RichInetSocketAddress =
    new RichInetSocketAddress(addr)

}
