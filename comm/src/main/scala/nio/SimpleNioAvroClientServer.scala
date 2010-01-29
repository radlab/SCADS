package edu.berkeley.cs.scads.comm

import java.nio.channels.SocketChannel
import java.net.InetSocketAddress
import java.util.concurrent.{Executor,Executors}

import org.apache.log4j.BasicConfigurator

class SimpleNioAvroServer(addr: InetSocketAddress, exec: Executor) extends NioAvroServer[Record,Record](addr,exec) {
    override def receiveMessage(src: RemoteNode, message: Record):Unit = {
        println("source: " + src)
        println("message: " + message)
    }
}

object SimpleNioAvroServer {

    BasicConfigurator.configure

    def main(args: Array[String]) = {
        val port = args(0).toInt
        val server = new SimpleNioAvroServer(new InetSocketAddress("localhost",port), Executors.newCachedThreadPool) 
        server.serve
    }

}

class SimpleNioAvroClient(exec: Executor) extends NioAvroClient[Record,Record](exec) {
    override def receiveMessage(src: RemoteNode, message: Record):Unit = {
        println("source: " + src)
        println("message: " + message)
    }
}

object SimpleNioAvroClient {

    BasicConfigurator.configure

    def main(args: Array[String]) = {
        val port = args(0).toInt
        val client = new SimpleNioAvroClient(Executors.newCachedThreadPool) 
        client.connect(new InetSocketAddress("localhost",port)).await
    }

}
