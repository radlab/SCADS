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

import java.util.concurrent.{Executor,Executors}

import org.apache.log4j.Logger

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

class NioClient(
        readExecutor: Executor,
        channelHandler: ChannelHandler)
    extends AbstractNioEndpoint(
        readExecutor,
        channelHandler) {

    val logger = Logger.getLogger("NioClient")

    private var initialized = false

    /**
     * Connect is thread safe
     */
    def connect(hostAddress: InetSocketAddress):ConnectFuture = {
        synchronized {
            if (!initialized) {
                fireWriteAndSelectLoop
                initialized = true
            }
        }
        val clientSocket = SocketChannel.open
        clientSocket.configureBlocking(false)
        val connected = clientSocket.connect(hostAddress)
        val future = new ConnectFuture(clientSocket, connected)
        if (connected) {
            logger.info("Connected!")
        } else {
            logger.info("Not connected, deferring connection")
            //clientSocket.register(selector, SelectionKey.OP_CONNECT, future)
            registerQueue.synchronized {
                registerQueue.add(new RegisterEntry(clientSocket, future))
            }
        }
        logger.debug("waking up selector")
        selector.wakeup
        registerInetSocketAddress(hostAddress, clientSocket)
        future
    }

    override protected def connect(key: SelectionKey):Unit = {  
        logger.debug("connect() called with: " + key)
        val socketChannel = key.channel.asInstanceOf[SocketChannel]
        try {
            socketChannel.finishConnect
        } catch {
            case e: IOException =>
                // Cancel the channel's registration with our selector
                key.cancel
                return
        }
        key.interestOps(SelectionKey.OP_READ)
        key.attachment.asInstanceOf[ConnectFuture].finished
    }

}
