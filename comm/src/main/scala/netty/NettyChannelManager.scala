package edu.berkeley.cs.scads.comm
package netty

import java.net._
import java.util.concurrent._

import org.jboss.netty._
import bootstrap._
import channel._
import handler.codec.frame._
import socket.nio._

import net.lag.logging.Logger

import org.apache.avro.specific.SpecificRecord

import scala.reflect.Manifest.classType
import edu.berkeley.cs.scads.config._

/**
 * Easier to instantiate via reflection
 */
class DefaultNettyChannelManager[S <: SpecificRecord, R <: SpecificRecord](
    recvMsg: (AvroChannelManager[S, R], RemoteNode, R) => Unit, sendClz: Class[S], recvClz: Class[R]) 
  extends NettyChannelManager[S, R]()(classType(sendClz), classType(recvClz)) {
  override def receiveMessage(remoteNode: RemoteNode, msg: R) {
    recvMsg(this, remoteNode, msg)
  }
}

abstract class NettyChannelManager[S <: SpecificRecord, R <: SpecificRecord](
    implicit sendManifest: Manifest[S], recvManifest: Manifest[R])
  extends AvroChannelManager[S, R] {

  protected val log = Logger()

  private lazy val useTcpNoDelay = Config.config.getBool("scads.comm.tcpNoDelay", true)

  private def pipelineFactory(handler: ChannelHandler) = new ChannelPipelineFactory {
    override def getPipeline() =
      new StaticChannelPipeline(
        new LengthFieldPrepender(4),
        new LengthFieldBasedFrameDecoder(0x0fffffff, 0, 4, 0, 4),
        new AvroSpecificDecoder[R],
        new AvroSpecificEncoder[S],
        handler)
  }

  /** TODO: configure thread pools */
  private val serverBootstrap = new ServerBootstrap(
    new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool()))

  serverBootstrap.setParentHandler(new NettyServerParentHandler)
  serverBootstrap.setPipelineFactory(pipelineFactory(new NettyServerChildHandler))
  serverBootstrap.setOption("child.tcpNoDelay", useTcpNoDelay) // disable nagle's algorithm

  /** TODO: configure thread pools */
  private val clientBootstrap = new ClientBootstrap(
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool()))

  clientBootstrap.setPipelineFactory(pipelineFactory(new NettyClientHandler))
  clientBootstrap.setOption("tcpNoDelay", useTcpNoDelay) // disable nagle's algorithm

  class NettyBaseHandler extends SimpleChannelHandler {
    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.error("Handler caught exception", e.getCause)
      e.getChannel.close()
      ctx.sendUpstream(e)
    }
  }

  class NettyChannelHandler extends NettyBaseHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val msg  = e.getMessage.asInstanceOf[R]
      val node = e.getRemoteAddress.asInstanceOf[InetSocketAddress]
      receiveMessage(RemoteNode(node.getHostName, node.getPort), msg)
    }
  }

  class NettyServerParentHandler extends NettyBaseHandler {
    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      val chan = e.getChannel
      log.info("Listener closed: %s".format(chan))
      if (chan.getLocalAddress ne null)
        portToListeners.remove(chan.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)
      ctx.sendUpstream(e)
    }
  }

  class NettyServerChildHandler extends NettyChannelHandler {
    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      val chan = e.getChannel
      log.info("Received new connection from %s".format(chan))
      if (chan.getRemoteAddress ne null)
        nodeToConnections.put(chan.getRemoteAddress.asInstanceOf[InetSocketAddress], chan)
      ctx.sendUpstream(e)
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      val chan = e.getChannel
      log.info("Closing connection from %s".format(chan))
      if (chan.getRemoteAddress ne null)
        nodeToConnections.remove(chan.getRemoteAddress.asInstanceOf[InetSocketAddress])
      ctx.sendUpstream(e)
    }
  }

  class NettyClientHandler extends NettyChannelHandler {
    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      val chan = e.getChannel
      log.info("Channel closed: %s".format(chan))
      if (chan.getRemoteAddress ne null)
        nodeToConnections.remove(chan.getRemoteAddress.asInstanceOf[InetSocketAddress])
      ctx.sendUpstream(e)
    }
  }

  // TODO: refactor commonality between this and OIO handler

  /** Used to manage open connections */
  private val nodeToConnections = 
    new ConcurrentHashMap[InetSocketAddress, Channel]

  /** Used to manage open ports */
  private val portToListeners =
    new ConcurrentHashMap[Int, Channel]

  override def sendMessage(dest: RemoteNode, msg: S) {
    val conn = getConnectionFor(dest)
    conn.write(msg) // encoding done in channel handlers
  }

  override def sendMessageBulk(dest: RemoteNode, msg: S) {
    // TODO: implement me properly
    sendMessage(dest, msg)
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

  private def connect(addr: InetSocketAddress) = {
    require(addr ne null)

    log.info("opening new connection to address: %s".format(addr))
    // open the connection in the current thread
    clientBootstrap.connect(addr).awaitUninterruptibly.getChannel
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
    serverBootstrap.bind(new InetSocketAddress(port))
  }

}
