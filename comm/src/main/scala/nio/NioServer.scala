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
import java.util.{List => JList}

import java.util.concurrent.Executor

import org.apache.log4j.Logger

class NioServer(
        protected val hostAddress: InetSocketAddress, 
        readExecutor: Executor,
        channelHandler: ChannelHandler)
    extends AbstractNioEndpoint(
        readExecutor,
        channelHandler) {
    
    private var logger = Logger.getLogger("NioServer")

	protected var serverChannel: ServerSocketChannel = null

    def serve = {
        serverChannel = ServerSocketChannel.open
		serverChannel.configureBlocking(false)
        serverChannel.socket.bind(hostAddress)
		serverChannel.register(selector, SelectionKey.OP_ACCEPT)
        fireWriteAndSelectLoop
        logger.info("Now serving on: " + hostAddress)
    }

    override protected def accept(key: SelectionKey):Unit = {
        logger.debug("accept() called with: " + key)
        val serverSocketChannel = key.channel.asInstanceOf[ServerSocketChannel]
		val socketChannel = serverSocketChannel.accept
		socketChannel.configureBlocking(false)
		socketChannel.register(selector, SelectionKey.OP_READ)
        registerInetSocketAddress(new InetSocketAddress(socketChannel.socket.getInetAddress, socketChannel.socket.getPort), socketChannel)
    }

}
