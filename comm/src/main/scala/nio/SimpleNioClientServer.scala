package edu.berkeley.cs.scads.comm

import java.nio.channels.SocketChannel
import java.net.InetSocketAddress
import java.util.concurrent.Executors

import org.apache.log4j.BasicConfigurator

class SimpleChannelHandler extends ChannelHandler {

    def processData(socket: SocketChannel, data: Array[Byte], count: Int) = {
        println("Got " + count + " bytes")
        println(new String(data))
    }

    def handleException(exception: Exception) = {
        println(exception.getMessage)
    }

}

object SimpleNioServer {

    BasicConfigurator.configure

    def main(args: Array[String]) = {
        val port = args(0).toInt
        val server = new NioServer(new InetSocketAddress("localhost",port), Executors.newCachedThreadPool, new SimpleChannelHandler) 
        server.serve
    }
}

object SimpleNioClient {

    BasicConfigurator.configure
    
    def main(args: Array[String]):Unit = {
        val port = args(0).toInt
        val client = new NioClient(new InetSocketAddress("localhost",port), Executors.newCachedThreadPool, new SimpleChannelHandler) 
        val future = client.connect
        future.await
        println("Connected!")
        val socket = future.clientSocket
        (1 to 100).foreach( i => {
            client.send(socket, new String("Message: " + i).getBytes, true)
        })

    }

}

